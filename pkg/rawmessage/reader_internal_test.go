package rawmessage

import (
	"bytes"
	"testing"

	"github.com/bluenviron/gortmplib/pkg/chunk"
	"github.com/stretchr/testify/require"
)

func TestReaderCoalescesTinyIncompleteChunks(t *testing.T) {
	const payload = 64 << 10
	first, err := (chunk.Chunk0{ChunkStreamID: 2, BodyLen: payload + 1, Type: 9, Body: []byte{1}}).Marshal(false)
	require.NoError(t, err)
	var wire bytes.Buffer
	_, err = wire.Write(first)
	require.NoError(t, err)
	for range payload - 1 {
		_, err = wire.Write([]byte{0xc2, 1})
		require.NoError(t, err)
	}
	marker, err := (chunk.Chunk0{ChunkStreamID: 3, BodyLen: 1, Type: 4, Body: []byte{2}}).Marshal(false)
	require.NoError(t, err)
	_, err = wire.Write(marker)
	require.NoError(t, err)
	r := &Reader{BR: &wire}
	r.Initialize()
	require.NoError(t, r.SetChunkSize(1))
	_, err = r.Read()
	require.NoError(t, err)
	stream := r.chunkStreams[2]
	require.Len(t, stream.curBody, payload)
	require.LessOrEqual(t, cap(stream.curBody), 2*payload)
	require.Equal(t, uint64(cap(stream.curBody)), r.incompleteBytes)
}

func TestReaderDoesNotReserveDeclaredBodyFromOneByte(t *testing.T) {
	first, err := (chunk.Chunk0{ChunkStreamID: 2, BodyLen: 10 << 20, Type: 9, Body: []byte{1}}).Marshal(false)
	require.NoError(t, err)
	marker, err := (chunk.Chunk0{ChunkStreamID: 3, BodyLen: 1, Type: 4, Body: []byte{2}}).Marshal(false)
	require.NoError(t, err)
	r := &Reader{BR: bytes.NewReader(append(first, marker...))}
	r.Initialize()
	require.NoError(t, r.SetChunkSize(1))
	_, err = r.Read()
	require.NoError(t, err)
	require.LessOrEqual(t, cap(r.chunkStreams[2].curBody), 128)
}
