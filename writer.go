package ddsmetadata

import (
	"os"
)

// bufferWriter knows how to copy data of given length to arena.
// 这里的结构可有可无，因为目前只有bytesWriter，并没有stringWriter函数
type bufferWriter interface {
	// len returns the length of the data that bufferWriter holds.
	len() int

	// writeTo writes the data to arena starting at given offset. It is possible that the
	// whole data that bufferWriter holds may not fit in the given arena. Hence, an index
	// into the data is provided. The data is copied starting from index until either
	// no more data is left, or no space is left in the given arena to write more data.
	writeTo(aa *os.File, offset, index int) int
}

// bytesWriter holds a slice of bytes and satisfies the bigqueue.writer interface.
type bytesWriter struct {
	b []byte
}

// len returns the length of data that bytesWriter holds.
func (bw *bytesWriter) len() int {
	return len(bw.b)
}

// writeTo writes data that it holds from index to end of
// the data or arena, into the arena starting at the offset.
func (bw *bytesWriter) writeTo(aa *os.File, offset, index int) int {
	n, _ := aa.WriteAt(bw.b[index:], int64(offset))
	return n
}
