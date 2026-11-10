package chunk

import "io"

// Read large chunks incrementally so a header alone cannot reserve the
// advertised body size. Normal-sized chunks keep the direct allocation path.
func readBody(r io.Reader, size uint32) ([]byte, error) {
	const blockSize = 64 * 1024
	if size <= blockSize {
		body := make([]byte, size)
		_, err := io.ReadFull(r, body)
		return body, err
	}
	var parts [][]byte
	var received uint32
	for received < size/2+size%2 {
		part := make([]byte, min(size-received, blockSize))
		if _, err := io.ReadFull(r, part); err != nil {
			return nil, err
		}
		parts = append(parts, part)
		received += uint32(len(part))
	}
	body := make([]byte, size)
	var offset int
	for _, part := range parts {
		offset += copy(body[offset:], part)
	}
	_, err := io.ReadFull(r, body[offset:])
	return body, err
}
