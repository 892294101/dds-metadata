package ddsmetadata

import (
	"os"
)

// reader knows how to read data from arena.
type reader interface {
	// grow grows reader's capacity, if necessary, to guarantee space for
	// another n bytes. After grow(n), at least n bytes can be written to reader
	// without another allocation. If n is negative, grow panics.
	grow(n int)

	// readFrom copies data from arena starting at given offset. Because the data
	// may be spread over multiple arenas, an index into the data is provided so
	// the data is copied starting at given index.
	readFrom(aa *os.File, offset uint64, index int) int
}

// bytesReader holds a slice of bytes to hold the data.
type bytesReader struct {
	b []byte
}

// grow expands the capacity of bytesReader by n bytes.
// 这里的结构可有可无，因为目前只有bytesWriter，并没有stringWriter函数
func (br *bytesReader) grow(n int) {
	if n < 0 {
		panic("cache.reader.grow: negative count")
	}

	temp := make([]byte, n+len(br.b))
	if br.b != nil {
		_ = copy(temp, br.b)
	}
	br.b = temp
}

// readFrom reads the arena at offset and copies the data at index.
func (br *bytesReader) readFrom(aa *os.File, offset uint64, index int) int {
	n, _ := aa.ReadAt(br.b[index:], int64(offset))
	return n
}
