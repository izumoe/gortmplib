package rawmessage

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/bluenviron/gortmplib/pkg/bytecounter"
	"github.com/bluenviron/gortmplib/pkg/chunk"
)

var errMoreChunksNeeded = errors.New("more chunks are needed")

const (
	maxBodySize        = 10 * 1024 * 1024
	maxIncompleteBytes = 16 * 1024 * 1024
)

func joinFragments(fragments, final []byte, size uint32) []byte {
	ret := make([]byte, size)
	n := copy(ret, fragments)
	copy(ret[n:], final)
	return ret
}

type readerChunkStream struct {
	mr                    *Reader
	curTimestamp          uint32
	curTimestampAvailable bool
	curType               uint8
	curMessageStreamID    uint32
	curBodyLen            uint32
	curBody               []byte
	curBodyRecv           uint32
	curTimestampDelta     uint32
	hasExtendedTimestamp  bool
}

func (rc *readerChunkStream) abortMessage() {
	rc.curBodyLen = 0
	rc.releaseFragments()
}

func (rc *readerChunkStream) releaseFragments() {
	rc.mr.incompleteBytes -= uint64(cap(rc.curBody))
	rc.curBody = nil
	rc.curBodyRecv = 0
}

func (rc *readerChunkStream) addFragment(body []byte) error {
	needed := len(rc.curBody) + len(body)
	if needed > cap(rc.curBody) {
		available := int(maxIncompleteBytes - rc.mr.incompleteBytes + uint64(cap(rc.curBody)))
		if needed > available {
			return fmt.Errorf("incomplete message budget exceeded (%d bytes)", maxIncompleteBytes)
		}
		capacity := max(128, cap(rc.curBody)*2, needed)
		capacity = min(capacity, int(rc.curBodyLen), available)
		bodyBuffer := make([]byte, len(rc.curBody), capacity)
		copy(bodyBuffer, rc.curBody)
		rc.mr.incompleteBytes += uint64(capacity - cap(rc.curBody))
		rc.curBody = bodyBuffer
	}
	rc.curBody = append(rc.curBody, body...)
	rc.curBodyRecv += uint32(len(body))
	return nil
}

func (rc *readerChunkStream) readChunk(c chunk.Chunk, bodySize uint32, hasExtendedTimestamp bool) error {
	err := c.Read(rc.mr.bufr, bodySize, hasExtendedTimestamp)
	if err != nil {
		return err
	}

	// check if an ack is needed
	if rc.mr.ackWindowSize != 0 {
		count := uint32(rc.mr.BCR.Count())
		diff := count - rc.mr.LastAckCount

		if diff > rc.mr.ackWindowSize {
			err = rc.mr.OnAckNeeded(count)
			if err != nil {
				return err
			}

			rc.mr.LastAckCount += rc.mr.ackWindowSize
		}
	}

	return nil
}

func (rc *readerChunkStream) readMessage(typ byte) (*Message, error) {
	switch typ {
	case 0:
		if rc.curBodyRecv != 0 {
			return nil, fmt.Errorf("received type 0 chunk but expected type 3 chunk")
		}

		err := rc.readChunk(&rc.mr.c0, rc.mr.chunkSize, false)
		if err != nil {
			return nil, err
		}

		rc.curMessageStreamID = rc.mr.c0.MessageStreamID
		rc.curType = rc.mr.c0.Type
		rc.curTimestamp = rc.mr.c0.Timestamp
		rc.curTimestampAvailable = true
		rc.curTimestampDelta = 0
		rc.curBodyLen = rc.mr.c0.BodyLen
		rc.hasExtendedTimestamp = rc.mr.c0.Timestamp >= 0xFFFFFF

		if rc.curBodyLen > maxBodySize {
			return nil, fmt.Errorf("body size (%d) exceeds maximum (%d)", rc.curBodyLen, maxBodySize)
		}

		le := uint32(len(rc.mr.c0.Body))

		if rc.mr.c0.BodyLen != le {
			if err := rc.addFragment(rc.mr.c0.Body); err != nil {
				return nil, err
			}
			return nil, errMoreChunksNeeded
		}

		rc.mr.msg.Timestamp = time.Duration(rc.mr.c0.Timestamp) * time.Millisecond
		rc.mr.msg.Type = rc.mr.c0.Type
		rc.mr.msg.MessageStreamID = rc.mr.c0.MessageStreamID
		rc.mr.msg.Body = rc.mr.c0.Body
		return rc.mr.msg.clone(), nil

	case 1:
		if !rc.curTimestampAvailable {
			return nil, fmt.Errorf("received type 1 chunk without previous chunk")
		}

		if rc.curBodyRecv != 0 {
			return nil, fmt.Errorf("received type 1 chunk but expected type 3 chunk")
		}

		err := rc.readChunk(&rc.mr.c1, rc.mr.chunkSize, false)
		if err != nil {
			return nil, err
		}

		rc.curType = rc.mr.c1.Type
		rc.curTimestamp += rc.mr.c1.TimestampDelta
		rc.curTimestampDelta = rc.mr.c1.TimestampDelta
		rc.curBodyLen = rc.mr.c1.BodyLen
		rc.hasExtendedTimestamp = rc.mr.c1.TimestampDelta >= 0xFFFFFF

		if rc.curBodyLen > maxBodySize {
			return nil, fmt.Errorf("body size (%d) exceeds maximum (%d)", rc.curBodyLen, maxBodySize)
		}

		le := uint32(len(rc.mr.c1.Body))

		if rc.mr.c1.BodyLen != le {
			if err := rc.addFragment(rc.mr.c1.Body); err != nil {
				return nil, err
			}
			return nil, errMoreChunksNeeded
		}

		rc.mr.msg.Timestamp = time.Duration(rc.curTimestamp) * time.Millisecond
		rc.mr.msg.Type = rc.mr.c1.Type
		rc.mr.msg.MessageStreamID = rc.curMessageStreamID
		rc.mr.msg.Body = rc.mr.c1.Body
		return rc.mr.msg.clone(), nil

	case 2:
		if !rc.curTimestampAvailable {
			return nil, fmt.Errorf("received type 2 chunk without previous chunk")
		}

		if rc.curBodyRecv != 0 {
			return nil, fmt.Errorf("received type 2 chunk but expected type 3 chunk")
		}

		chunkBodyLen := min(rc.curBodyLen, rc.mr.chunkSize)

		err := rc.readChunk(&rc.mr.c2, chunkBodyLen, false)
		if err != nil {
			return nil, err
		}

		rc.curTimestamp += rc.mr.c2.TimestampDelta
		rc.curTimestampDelta = rc.mr.c2.TimestampDelta
		rc.hasExtendedTimestamp = rc.mr.c2.TimestampDelta >= 0xFFFFFF

		le := uint32(len(rc.mr.c2.Body))

		if rc.curBodyLen != le {
			if err := rc.addFragment(rc.mr.c2.Body); err != nil {
				return nil, err
			}
			return nil, errMoreChunksNeeded
		}

		rc.mr.msg.Timestamp = time.Duration(rc.curTimestamp) * time.Millisecond
		rc.mr.msg.Type = rc.curType
		rc.mr.msg.MessageStreamID = rc.curMessageStreamID
		rc.mr.msg.Body = rc.mr.c2.Body
		return rc.mr.msg.clone(), nil

	default: // 3
		if rc.curBodyRecv != 0 {
			chunkBodyLen := min(rc.curBodyLen-rc.curBodyRecv, rc.mr.chunkSize)

			err := rc.readChunk(&rc.mr.c3, chunkBodyLen, rc.hasExtendedTimestamp)
			if err != nil {
				return nil, err
			}

			if rc.curBodyLen != rc.curBodyRecv+uint32(len(rc.mr.c3.Body)) {
				if err := rc.addFragment(rc.mr.c3.Body); err != nil {
					return nil, err
				}
				return nil, errMoreChunksNeeded
			}

			rc.mr.msg.Timestamp = time.Duration(rc.curTimestamp) * time.Millisecond
			rc.mr.msg.Type = rc.curType
			rc.mr.msg.MessageStreamID = rc.curMessageStreamID
			if len(rc.curBody)+len(rc.mr.c3.Body) <= cap(rc.curBody) {
				rc.mr.msg.Body = append(rc.curBody, rc.mr.c3.Body...)
			} else {
				rc.mr.msg.Body = joinFragments(rc.curBody, rc.mr.c3.Body, rc.curBodyLen)
			}
			rc.releaseFragments()
			return rc.mr.msg.clone(), nil
		}

		if !rc.curTimestampAvailable {
			return nil, fmt.Errorf("received type 3 chunk without previous chunk")
		}

		chunkBodyLen := min(rc.curBodyLen, rc.mr.chunkSize)

		err := rc.readChunk(&rc.mr.c3, chunkBodyLen, rc.hasExtendedTimestamp)
		if err != nil {
			return nil, err
		}

		rc.curTimestamp += rc.curTimestampDelta

		le := uint32(len(rc.mr.c3.Body))

		if rc.curBodyLen != le {
			if err := rc.addFragment(rc.mr.c3.Body); err != nil {
				return nil, err
			}
			return nil, errMoreChunksNeeded
		}

		rc.mr.msg.Timestamp = time.Duration(rc.curTimestamp) * time.Millisecond
		rc.mr.msg.Type = rc.curType
		rc.mr.msg.MessageStreamID = rc.curMessageStreamID
		rc.mr.msg.Body = rc.mr.c3.Body
		return rc.mr.msg.clone(), nil
	}
}

// Reader is a raw message reader.
type Reader struct {
	BR           io.Reader
	BCR          *bytecounter.Reader
	OnAckNeeded  func(uint32) error
	LastAckCount uint32

	bufr            *bufio.Reader
	chunkSize       uint32
	ackWindowSize   uint32
	msg             Message
	c0              chunk.Chunk0
	c1              chunk.Chunk1
	c2              chunk.Chunk2
	c3              chunk.Chunk3
	chunkStreams    map[byte]*readerChunkStream
	incompleteBytes uint64
}

// Initialize initializes the Reader.
func (r *Reader) Initialize() {
	r.bufr = bufio.NewReader(r.BR)
	r.chunkSize = 128
	r.chunkStreams = make(map[byte]*readerChunkStream)
	r.incompleteBytes = 0
}

// NewReader allocates a Reader.
//
// Deprecated: use Initialize() instead.
func NewReader(
	br io.Reader,
	bcr *bytecounter.Reader,
	onAckNeeded func(uint32) error,
) *Reader {
	r := &Reader{
		BR:          br,
		BCR:         bcr,
		OnAckNeeded: onAckNeeded,
	}
	r.Initialize()
	return r
}

// SetChunkSize sets the maximum chunk size.
func (r *Reader) SetChunkSize(v uint32) error {
	if v == 0 {
		return fmt.Errorf("chunk size must be positive")
	}
	if v > maxBodySize {
		return fmt.Errorf("chunk size (%d) exceeds maximum (%d)", v, maxBodySize)
	}

	r.chunkSize = v
	return nil
}

// SetWindowAckSize sets the window acknowledgement size.
func (r *Reader) SetWindowAckSize(v uint32) {
	r.ackWindowSize = v
}

// AbortChunkStream discards any in-progress message assembly state for a chunk stream.
func (r *Reader) AbortChunkStream(v uint32) {
	if v < 2 || v > 63 {
		return
	}

	rc, ok := r.chunkStreams[byte(v)]
	if !ok || rc.curBodyRecv == 0 {
		return
	}

	rc.abortMessage()
}

// Read reads a Message.
func (r *Reader) Read() (*Message, error) {
	for {
		byt, err := r.bufr.ReadByte()
		if err != nil {
			return nil, err
		}

		typ := byt >> 6
		chunkStreamID := byt & 0x3F

		if chunkStreamID < 2 {
			return nil, fmt.Errorf("extended chunk stream IDs are not supported (yet)")
		}

		rc, ok := r.chunkStreams[chunkStreamID]
		if !ok {
			rc = &readerChunkStream{mr: r}
			r.chunkStreams[chunkStreamID] = rc
		}

		r.bufr.UnreadByte() //nolint:errcheck
		// The message length in a type 0 or 1 header is available before
		// reading its body. Check the shared budget before allocating a chunk.
		var bodyLen uint32
		if typ == 0 || typ == 1 {
			header, err := r.bufr.Peek(7)
			if err != nil {
				return nil, err
			}
			bodyLen = uint32(header[4])<<16 | uint32(header[5])<<8 | uint32(header[6])
		} else {
			bodyLen = rc.curBodyLen
		}
		remaining := bodyLen
		if typ == 3 && rc.curBodyRecv != 0 {
			remaining -= rc.curBodyRecv
		}
		chunkLen := min(remaining, r.chunkSize)
		partial := bodyLen > r.chunkSize
		if typ == 3 && rc.curBodyRecv != 0 {
			partial = remaining > r.chunkSize
		}
		if partial {
			needed := uint64(len(rc.curBody)) + uint64(chunkLen)
			if needed > uint64(cap(rc.curBody)) && r.incompleteBytes+needed-uint64(cap(rc.curBody)) > maxIncompleteBytes {
				return nil, fmt.Errorf("incomplete message budget exceeded (%d bytes)", maxIncompleteBytes)
			}
		}

		msg, err := rc.readMessage(typ)
		if err != nil {
			if errors.Is(err, errMoreChunksNeeded) {
				continue
			}
			return nil, err
		}

		msg.ChunkStreamID = chunkStreamID

		return msg, err
	}
}
