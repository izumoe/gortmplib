package rawmessage_test

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bluenviron/gortmplib/pkg/bytecounter"
	"github.com/bluenviron/gortmplib/pkg/chunk"
	"github.com/bluenviron/gortmplib/pkg/rawmessage"
)

func TestReaderBoundsIncompleteMessagesAcrossChunkStreams(t *testing.T) {
	const chunkSize = 1 << 20
	var wire bytes.Buffer
	for stream := byte(2); stream < 19; stream++ {
		part, err := (chunk.Chunk0{ChunkStreamID: stream, BodyLen: 2 * chunkSize, Type: 9, Body: bytes.Repeat([]byte{stream}, chunkSize)}).Marshal(false)
		require.NoError(t, err)
		_, err = wire.Write(part)
		require.NoError(t, err)
	}
	r := &rawmessage.Reader{BR: &wire}
	r.Initialize()
	require.NoError(t, r.SetChunkSize(chunkSize))
	_, err := r.Read()
	require.ErrorContains(t, err, "incomplete message budget")
}

func TestReaderRejectsZeroChunkSize(t *testing.T) {
	r := &rawmessage.Reader{BR: bytes.NewReader(nil)}
	r.Initialize()
	require.Error(t, r.SetChunkSize(0))
}

func TestReaderRejectsOverBudgetChunkBeforeBody(t *testing.T) {
	const chunkSize = 1 << 20
	var wire bytes.Buffer
	for stream := byte(2); stream < 18; stream++ {
		part, err := (chunk.Chunk0{ChunkStreamID: stream, BodyLen: 2 * chunkSize, Type: 9, Body: bytes.Repeat([]byte{stream}, chunkSize)}).Marshal(false)
		require.NoError(t, err)
		_, err = wire.Write(part)
		require.NoError(t, err)
	}
	header, err := (chunk.Chunk0{ChunkStreamID: 18, BodyLen: 2 * chunkSize, Type: 9}).Marshal(false)
	require.NoError(t, err)
	_, err = wire.Write(header)
	require.NoError(t, err)
	r := &rawmessage.Reader{BR: &wire}
	r.Initialize()
	require.NoError(t, r.SetChunkSize(chunkSize))
	_, err = r.Read()
	require.ErrorContains(t, err, "incomplete message budget", "the reader must reject without waiting for the chunk body")
}

func TestReaderInitializeClearsIncompleteBudget(t *testing.T) {
	const chunkSize = 1 << 20
	var first bytes.Buffer
	for stream := byte(2); stream < 18; stream++ {
		part, err := (chunk.Chunk0{ChunkStreamID: stream, BodyLen: 2 * chunkSize, Type: 9, Body: bytes.Repeat([]byte{stream}, chunkSize)}).Marshal(false)
		require.NoError(t, err)
		_, err = first.Write(part)
		require.NoError(t, err)
	}
	r := &rawmessage.Reader{BR: &first}
	r.Initialize()
	require.NoError(t, r.SetChunkSize(chunkSize))
	_, err := r.Read()
	require.ErrorIs(t, err, io.EOF)
	part, err := (chunk.Chunk0{ChunkStreamID: 2, BodyLen: 2 * chunkSize, Type: 9, Body: bytes.Repeat([]byte{2}, chunkSize)}).Marshal(false)
	require.NoError(t, err)
	r.BR = bytes.NewReader(part)
	r.Initialize()
	require.NoError(t, r.SetChunkSize(chunkSize))
	_, err = r.Read()
	require.ErrorIs(t, err, io.EOF, "reinitialization must release the old budget")
}

func TestReaderReleasesIncompleteMessageBudgetOnAbort(t *testing.T) {
	const chunkSize = 1 << 20
	var wire bytes.Buffer
	appendChunk := func(c chunk.Chunk) {
		t.Helper()
		part, err := c.Marshal(false)
		require.NoError(t, err)
		_, err = wire.Write(part)
		require.NoError(t, err)
	}
	for stream := byte(2); stream < 18; stream++ {
		appendChunk(&chunk.Chunk0{ChunkStreamID: stream, BodyLen: 2 * chunkSize, Type: 9, Body: bytes.Repeat([]byte{stream}, chunkSize)})
	}
	appendChunk(&chunk.Chunk0{ChunkStreamID: 30, BodyLen: 1, Type: 4, Body: []byte{1}})
	for stream := byte(18); stream < 20; stream++ {
		appendChunk(&chunk.Chunk0{ChunkStreamID: stream, BodyLen: 2 * chunkSize, Type: 9, Body: bytes.Repeat([]byte{stream}, chunkSize)})
	}
	appendChunk(&chunk.Chunk0{ChunkStreamID: 31, BodyLen: 1, Type: 4, Body: []byte{2}})
	r := &rawmessage.Reader{BR: &wire}
	r.Initialize()
	require.NoError(t, r.SetChunkSize(chunkSize))
	first, err := r.Read()
	require.NoError(t, err)
	require.Equal(t, []byte{1}, first.Body)
	r.AbortChunkStream(2)
	r.AbortChunkStream(3)
	second, err := r.Read()
	require.NoError(t, err)
	require.Equal(t, []byte{2}, second.Body)
}

func TestReaderCompletesMessageAtFullIncompleteBudget(t *testing.T) {
	const chunkSize = 1 << 20
	var wire bytes.Buffer
	for stream := byte(2); stream < 18; stream++ {
		part, err := (chunk.Chunk0{ChunkStreamID: stream, BodyLen: 2 * chunkSize, Type: 9, Body: bytes.Repeat([]byte{stream}, chunkSize)}).Marshal(false)
		require.NoError(t, err)
		_, err = wire.Write(part)
		require.NoError(t, err)
	}
	last, err := (chunk.Chunk3{ChunkStreamID: 2, Body: bytes.Repeat([]byte{2}, chunkSize)}).Marshal(false)
	require.NoError(t, err)
	_, err = wire.Write(last)
	require.NoError(t, err)
	r := &rawmessage.Reader{BR: &wire}
	r.Initialize()
	require.NoError(t, r.SetChunkSize(chunkSize))
	message, err := r.Read()
	require.NoError(t, err, "a complete message must be able to release a full budget")
	require.Len(t, message.Body, 2*chunkSize)
	require.Equal(t, bytes.Repeat([]byte{2}, 2*chunkSize), message.Body)
}

var cases = []struct {
	name     string
	messages []*rawmessage.Message
	chunks   []chunk.Chunk
}{
	{
		"(chunk0) + (chunk1)",
		[]*rawmessage.Message{
			{
				ChunkStreamID:   27,
				Timestamp:       18576 * time.Millisecond,
				Type:            6,
				MessageStreamID: 3123,
				Body:            bytes.Repeat([]byte{0x03}, 64),
			},
			{
				ChunkStreamID:   27,
				Timestamp:       (18576 + 15) * time.Millisecond,
				Type:            5,
				MessageStreamID: 3123,
				Body:            bytes.Repeat([]byte{0x04}, 64),
			},
		},
		[]chunk.Chunk{
			&chunk.Chunk0{
				ChunkStreamID:   27,
				Timestamp:       18576,
				Type:            6,
				MessageStreamID: 3123,
				BodyLen:         64,
				Body:            bytes.Repeat([]byte{0x03}, 64),
			},
			&chunk.Chunk1{
				ChunkStreamID:  27,
				TimestampDelta: 15,
				Type:           5,
				BodyLen:        64,
				Body:           bytes.Repeat([]byte{0x04}, 64),
			},
		},
	},
	{
		"(chunk0) + (chunk2) + (chunk3)",
		[]*rawmessage.Message{
			{
				ChunkStreamID:   27,
				Timestamp:       18576 * time.Millisecond,
				Type:            6,
				MessageStreamID: 3123,
				Body:            bytes.Repeat([]byte{0x03}, 64),
			},
			{
				ChunkStreamID:   27,
				Timestamp:       (18576 + 15) * time.Millisecond,
				Type:            6,
				MessageStreamID: 3123,
				Body:            bytes.Repeat([]byte{0x04}, 64),
			},
			{
				ChunkStreamID:   27,
				Timestamp:       (18576 + 15 + 15) * time.Millisecond,
				Type:            6,
				MessageStreamID: 3123,
				Body:            bytes.Repeat([]byte{0x05}, 64),
			},
		},
		[]chunk.Chunk{
			&chunk.Chunk0{
				ChunkStreamID:   27,
				Timestamp:       18576,
				Type:            6,
				MessageStreamID: 3123,
				BodyLen:         64,
				Body:            bytes.Repeat([]byte{0x03}, 64),
			},
			&chunk.Chunk2{
				ChunkStreamID:  27,
				TimestampDelta: 15,
				Body:           bytes.Repeat([]byte{0x04}, 64),
			},
			&chunk.Chunk3{
				ChunkStreamID: 27,
				Body:          bytes.Repeat([]byte{0x05}, 64),
			},
		},
	},
	{
		"(chunk0 + chunk3) + (chunk1 + chunk3) + (chunk2 + chunk3) + (chunk3 + chunk3)",
		[]*rawmessage.Message{
			{
				ChunkStreamID:   27,
				Timestamp:       18576 * time.Millisecond,
				Type:            6,
				MessageStreamID: 3123,
				Body:            bytes.Repeat([]byte{0x03}, 190),
			},
			{
				ChunkStreamID:   27,
				Timestamp:       18576 * time.Millisecond,
				Type:            6,
				MessageStreamID: 3123,
				Body:            bytes.Repeat([]byte{0x04}, 192),
			},
			{
				ChunkStreamID:   27,
				Timestamp:       (18576 + 15) * time.Millisecond,
				Type:            6,
				MessageStreamID: 3123,
				Body:            bytes.Repeat([]byte{0x05}, 192),
			},
			{
				ChunkStreamID:   27,
				Timestamp:       (18576 + 15 + 15) * time.Millisecond,
				Type:            6,
				MessageStreamID: 3123,
				Body:            bytes.Repeat([]byte{0x06}, 192),
			},
		},
		[]chunk.Chunk{
			&chunk.Chunk0{
				ChunkStreamID:   27,
				Timestamp:       18576,
				Type:            6,
				MessageStreamID: 3123,
				BodyLen:         190,
				Body:            bytes.Repeat([]byte{0x03}, 128),
			},
			&chunk.Chunk3{
				ChunkStreamID: 27,
				Body:          bytes.Repeat([]byte{0x03}, 62),
			},
			&chunk.Chunk1{
				ChunkStreamID:  27,
				TimestampDelta: 0,
				Type:           6,
				BodyLen:        192,
				Body:           bytes.Repeat([]byte{0x04}, 128),
			},
			&chunk.Chunk3{
				ChunkStreamID: 27,
				Body:          bytes.Repeat([]byte{0x04}, 64),
			},
			&chunk.Chunk2{
				ChunkStreamID:  27,
				TimestampDelta: 15,
				Body:           bytes.Repeat([]byte{0x05}, 128),
			},
			&chunk.Chunk3{
				ChunkStreamID: 27,
				Body:          bytes.Repeat([]byte{0x05}, 64),
			},
			&chunk.Chunk3{
				ChunkStreamID: 27,
				Body:          bytes.Repeat([]byte{0x06}, 128),
			},
			&chunk.Chunk3{
				ChunkStreamID: 27,
				Body:          bytes.Repeat([]byte{0x06}, 64),
			},
		},
	},
	{
		"(chunk0 + chunk3 with extended timestamp)",
		[]*rawmessage.Message{
			{
				ChunkStreamID:   27,
				Timestamp:       0xFF123456 * time.Millisecond,
				Type:            6,
				MessageStreamID: 3123,
				Body:            bytes.Repeat([]byte{5}, 160),
			},
		},
		[]chunk.Chunk{
			&chunk.Chunk0{
				ChunkStreamID:   27,
				Timestamp:       4279383126,
				Type:            6,
				MessageStreamID: 3123,
				BodyLen:         160,
				Body:            bytes.Repeat([]byte{5}, 128),
			},
			&chunk.Chunk3{
				ChunkStreamID: 27,
				Body:          bytes.Repeat([]byte{5}, 32),
			},
		},
	},
	{
		"decreasing timestamp",
		[]*rawmessage.Message{
			{
				ChunkStreamID:   27,
				Timestamp:       16 * time.Second,
				Type:            6,
				MessageStreamID: 3123,
				Body:            []byte{1, 2},
			},
			{
				ChunkStreamID:   27,
				Timestamp:       17 * time.Second,
				Type:            6,
				MessageStreamID: 3123,
				Body:            []byte{3, 4},
			},
			{
				ChunkStreamID:   27,
				Timestamp:       16 * time.Second,
				Type:            6,
				MessageStreamID: 3123,
				Body:            []byte{5, 6},
			},
			{
				ChunkStreamID:   27,
				Timestamp:       17 * time.Second,
				Type:            6,
				MessageStreamID: 3123,
				Body:            []byte{7, 8},
			},
		},
		[]chunk.Chunk{
			&chunk.Chunk0{
				ChunkStreamID:   27,
				Timestamp:       16000,
				Type:            6,
				MessageStreamID: 3123,
				BodyLen:         2,
				Body:            []byte{1, 2},
			},
			&chunk.Chunk2{
				ChunkStreamID:  27,
				TimestampDelta: 1000,
				Body:           []byte{3, 4},
			},
			&chunk.Chunk0{
				ChunkStreamID:   27,
				Timestamp:       16000,
				Type:            6,
				MessageStreamID: 3123,
				BodyLen:         2,
				Body:            []byte{5, 6},
			},
			&chunk.Chunk2{
				ChunkStreamID:  27,
				TimestampDelta: 1000,
				Body:           []byte{7, 8},
			},
		},
	},
}

func TestReader(t *testing.T) {
	for _, ca := range cases { //nolint:dupl
		t.Run(ca.name, func(t *testing.T) {
			var buf bytes.Buffer
			br := bytecounter.NewReader(&buf)
			r := &rawmessage.Reader{
				BR:  br,
				BCR: br,
				OnAckNeeded: func(_ uint32) error {
					return nil
				},
			}
			r.Initialize()

			hasExtendedTimestamp := false

			for _, cach := range ca.chunks {
				buf2, err := cach.Marshal(hasExtendedTimestamp)
				require.NoError(t, err)
				buf.Write(buf2)
				hasExtendedTimestamp = chunkHasExtendedTimestamp(cach)
			}

			var msgs []*rawmessage.Message

			for {
				msg, err := r.Read()
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)
				msgs = append(msgs, msg)
			}

			require.Equal(t, ca.messages, msgs)
		})
	}
}

func TestReaderAdditional(t *testing.T) {
	for _, ca := range []struct {
		name     string
		messages []*rawmessage.Message
		chunks   []chunk.Chunk
	}{
		{
			"(chunk0) + (chunk3)",
			[]*rawmessage.Message{
				{
					ChunkStreamID:   27,
					Timestamp:       4279383126000000,
					Type:            6,
					MessageStreamID: 3123,
					Body:            bytes.Repeat([]byte{5}, 15),
				},
				{
					ChunkStreamID:   27,
					Timestamp:       4279383126000000,
					Type:            6,
					MessageStreamID: 3123,
					Body:            bytes.Repeat([]byte{6}, 15),
				},
			},
			[]chunk.Chunk{
				&chunk.Chunk0{
					ChunkStreamID:   27,
					Timestamp:       4279383126,
					Type:            6,
					MessageStreamID: 3123,
					BodyLen:         15,
					Body:            bytes.Repeat([]byte{5}, 15),
				},
				&chunk.Chunk3{
					ChunkStreamID: 27,
					Body:          bytes.Repeat([]byte{6}, 15),
				},
			},
		},
	} { //nolint:dupl
		t.Run(ca.name, func(t *testing.T) {
			var buf bytes.Buffer
			br := bytecounter.NewReader(&buf)
			r := &rawmessage.Reader{
				BR:  br,
				BCR: br,
				OnAckNeeded: func(_ uint32) error {
					return nil
				},
			}
			r.Initialize()

			hasExtendedTimestamp := false

			for _, cach := range ca.chunks {
				buf2, err := cach.Marshal(hasExtendedTimestamp)
				require.NoError(t, err)
				buf.Write(buf2)
				hasExtendedTimestamp = chunkHasExtendedTimestamp(cach)
			}

			var msgs []*rawmessage.Message

			for {
				msg, err := r.Read()
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)
				msgs = append(msgs, msg)
			}

			require.Equal(t, ca.messages, msgs)
		})
	}
}

func TestReaderAbortChunkStream(t *testing.T) {
	for _, ca := range []struct {
		name    string
		abortID uint32
	}{
		{
			name:    "unsupported low csid",
			abortID: 1,
		},
		{
			name:    "unsupported high csid",
			abortID: 64,
		},
		{
			name:    "known csid",
			abortID: 27,
		},
		{
			name:    "unknown csid",
			abortID: 28,
		},
	} {
		t.Run(ca.name, func(t *testing.T) {
			var buf bytes.Buffer
			bc := bytecounter.NewReader(&buf)
			r := &rawmessage.Reader{
				BR:  bc,
				BCR: bc,
				OnAckNeeded: func(_ uint32) error {
					return nil
				},
			}
			r.Initialize()
			_ = r.SetChunkSize(64)

			chunks := []chunk.Chunk{
				&chunk.Chunk0{
					ChunkStreamID:   27,
					Timestamp:       1000,
					Type:            6,
					MessageStreamID: 3123,
					BodyLen:         80,
					Body:            bytes.Repeat([]byte{0xaa}, 64),
				},
				&chunk.Chunk0{
					ChunkStreamID:   28,
					Timestamp:       2000,
					Type:            7,
					MessageStreamID: 4123,
					BodyLen:         4,
					Body:            []byte{1, 2, 3, 4},
				},
				&chunk.Chunk0{
					ChunkStreamID:   27,
					Timestamp:       3000,
					Type:            8,
					MessageStreamID: 3123,
					BodyLen:         3,
					Body:            []byte{9, 8, 7},
				},
			}

			hasExtendedTimestamp := false
			for _, cach := range chunks {
				buf2, err := cach.Marshal(hasExtendedTimestamp)
				require.NoError(t, err)
				buf.Write(buf2)
				hasExtendedTimestamp = chunkHasExtendedTimestamp(cach)
			}

			msg, err := r.Read()
			require.NoError(t, err)
			require.Equal(t, byte(28), msg.ChunkStreamID)
			require.Equal(t, []byte{1, 2, 3, 4}, msg.Body)

			r.AbortChunkStream(ca.abortID)

			msg, err = r.Read()
			if ca.abortID != 27 {
				require.EqualError(t, err, "received type 0 chunk but expected type 3 chunk")
				return
			}

			require.NoError(t, err)
			require.Equal(t, byte(27), msg.ChunkStreamID)
			require.Equal(t, []byte{9, 8, 7}, msg.Body)
		})
	}
}

func TestReaderAcknowledge(t *testing.T) {
	for _, ca := range []string{"standard", "overflow"} {
		t.Run(ca, func(t *testing.T) {
			onAckCalled := make(chan struct{})

			var buf bytes.Buffer
			bc := bytecounter.NewReader(&buf)
			r := &rawmessage.Reader{
				BR:  bc,
				BCR: bc,
				OnAckNeeded: func(_ uint32) error {
					close(onAckCalled)
					return nil
				},
			}
			r.Initialize()

			if ca == "overflow" {
				bc.SetCount(4294967096)
				r.LastAckCount = 4294967096
			}

			err := r.SetChunkSize(65536)
			require.NoError(t, err)

			r.SetWindowAckSize(100)

			buf2, err := chunk.Chunk0{
				ChunkStreamID:   27,
				Timestamp:       18576,
				Type:            6,
				MessageStreamID: 3123,
				BodyLen:         200,
				Body:            bytes.Repeat([]byte{0x03}, 200),
			}.Marshal(false)
			require.NoError(t, err)
			buf.Write(buf2)

			_, err = r.Read()
			require.NoError(t, err)

			<-onAckCalled
		})
	}
}

func FuzzReader(f *testing.F) {
	f.Fuzz(func(t *testing.T, b []byte) {
		bcr := bytecounter.NewReader(bytes.NewReader(b))
		r := &rawmessage.Reader{
			BR:  bcr,
			BCR: bcr,
			OnAckNeeded: func(_ uint32) error {
				return nil
			},
		}
		r.Initialize()

		var buf bytes.Buffer
		bcw := bytecounter.NewWriter(&buf)
		w := &rawmessage.Writer{
			BW:               bcw,
			BCW:              bcw,
			CheckAcknowledge: true,
		}
		w.Initialize()

		for {
			msg, err := r.Read()
			if err == nil {
				err = w.Write(msg)
				require.NoError(t, err)
			} else {
				break
			}
		}
	})
}
