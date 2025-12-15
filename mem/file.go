package mem

import (
	"io"
	"sort"
	"sync"
	"unsafe"

	"github.com/dacapoday/smol"
)

// File is an in-memory implementation of the smol.File interface.
// It is safe for concurrent use by multiple goroutines.
//
// File requires no initialization - just declare and use:
//
//	var f File
//	f.WriteAt([]byte("hello"), 0)
type File struct {
	rw       sync.RWMutex
	segments segments
}

var _ smol.File = new(File)

// Close clears all data stored in the File and releases memory.
// After Close, the file size becomes 0.
// It is safe to write to the file again after closing.
func (file *File) Close() error {
	file.rw.Lock()
	file.segments.close()
	file.rw.Unlock()
	return nil
}

// Size returns the current size of the file in bytes.
// This is a thread-safe operation.
func (file *File) Size() int64 {
	file.rw.RLock()
	defer file.rw.RUnlock()
	return file.segments.size()
}

const segmentSize = 32 * 1024

// ReadFrom reads data from r until EOF and replaces the entire file content.
// It implements io.ReaderFrom interface.
//
// Any existing data in the file is discarded.
//
// ReadFrom returns the number of bytes read and any error encountered,
// except that io.EOF is not returned as an error.
func (file *File) ReadFrom(r io.Reader) (n int64, err error) {
	file.rw.Lock()
	defer file.rw.Unlock()
	// Clear existing segments
	file.segments = nil
	for {
		buf := make([]byte, segmentSize)
		c, err := r.Read(buf)
		if c > 0 {
			n += int64(c)
			file.segments = append(file.segments, segment{
				seg: unsafe.Pointer(unsafe.SliceData(buf)),
				off: n,
			})
		}
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return n, err
		}
	}
}

// WriteTo writes the entire file content to w.
// It implements io.WriterTo interface.
//
// WriteTo acquires an exclusive lock to ensure a consistent snapshot,
// as concurrent writes may modify the internal segment structure.
//
// Returns the number of bytes written and any error encountered.
func (file *File) WriteTo(w io.Writer) (n int64, err error) {
	file.rw.Lock()
	defer file.rw.Unlock()
	// Write each segment sequentially
	for i := range file.segments {
		seg := file.segments.seg(i)
		c, err := w.Write(seg)
		n += int64(c)
		if err != nil {
			return n, err
		}
	}
	return
}

// WriteAt writes len(p) bytes from p to the file starting at byte offset off.
// It implements io.WriterAt interface.
//
// If the write position extends beyond the current file size, the file
// is automatically grown and the gap is filled with zero bytes.
//
// WriteAt returns the number of bytes written (always len(p) if err is nil)
// and any error encountered. It returns an error if off is negative.
//
// WriteAt is safe for concurrent use, but concurrent writes to the same
// memory location will cause data races.
func (file *File) WriteAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if len(p) == 0 {
		return 0, nil
	}

	file.rw.RLock()
	size := off + int64(len(p))
	// Check if we need to grow the file
	if bias := size - file.segments.size(); bias > 0 {
		// Upgrade to write lock to extend the file
		file.rw.RUnlock()
		file.rw.Lock()
		// Double-check after acquiring write lock
		if bias := size - file.segments.size(); bias > 0 {
			seg := make([]byte, bias)
			file.segments = append(file.segments, segment{
				seg: unsafe.Pointer(unsafe.SliceData(seg)),
				off: size,
			})
		}
		file.rw.Unlock()
		file.rw.RLock()
	}
	defer file.rw.RUnlock()
	return file.segments.writeAt(p, off)
}

// ReadAt reads len(p) bytes into p starting at byte offset off in the file.
// It implements io.ReaderAt interface.
//
// ReadAt returns the number of bytes read and any error encountered.
//
// This is a thread-safe operation that can run concurrently with other reads.
func (file *File) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if len(p) == 0 {
		return 0, nil
	}

	file.rw.RLock()
	defer file.rw.RUnlock()
	return file.segments.readAt(p, off)
}

// Truncate changes the size of the file.
//
// If the new size is smaller than the current size, the extra data is discarded.
// If the new size is larger, the file is extended and the new space is filled
// with zero bytes.
func (file *File) Truncate(size int64) error {
	file.rw.Lock()
	file.segments.truncate(size)
	file.rw.Unlock()
	return nil
}

// Sync is a no-op for in-memory files.
// It exists only to satisfy the smol.File interface and always returns nil.
func (file *File) Sync() error {
	return nil
}

type segments []segment

type segment = struct {
	seg unsafe.Pointer // pointer to the data buffer
	off int64          // cumulative offset (end position of this segment)
}

func (s *segments) close() {
	// for i := range len(*s) {
	// 	(*s)[i].seg = nil
	// }
	*s = nil
}

func (s segments) size() int64 {
	l := len(s)
	if l == 0 {
		return 0
	}
	return s[l-1].off
}

func (s segments) seek(off int64) int {
	return sort.Search(len(s), func(i int) bool {
		return s[i].off > off
	})
}

func (s segments) seg(idx int) []byte {
	if idx == 0 {
		return unsafe.Slice((*byte)(s[0].seg), s[0].off)
	}
	return unsafe.Slice((*byte)(s[idx].seg), s[idx].off-s[idx-1].off)
}

func (s segments) segOff(idx int, off int64) []byte {
	if idx == 0 {
		return unsafe.Slice((*byte)(unsafe.Add(s[0].seg, off)), s[0].off-off)
	}
	return unsafe.Slice((*byte)(unsafe.Add(s[idx].seg, off-s[idx-1].off)), s[idx].off-off)
}

func (s *segments) truncate(size int64) {
	bias := size - s.size()
	if bias > 0 {
		seg := make([]byte, bias)
		*s = append(*s, segment{
			seg: unsafe.Pointer(unsafe.SliceData(seg)),
			off: size,
		})
	} else if bias < 0 {
		idx := s.seek(size)
		(*s)[idx].off = size
		idx++
		// for i := idx; i < len(*s); i++ {
		// 	(*s)[i].seg = nil
		// }
		*s = (*s)[:idx]
	}
}

func (s *segments) writeAt(p []byte, off int64) (n int, err error) {
	var data []byte
	idx := s.seek(off)
	if (*s)[idx].off > off {
		data = s.segOff(idx, off)
		n = copy(data, p)
		if n == len(p) {
			return n, nil
		}
		p = p[n:]
	}
	for {
		idx++
		data = s.seg(idx)
		c := copy(data, p)
		n += c
		if c == len(p) {
			return n, nil
		}
		p = p[c:]
	}
}

func (s segments) readAt(p []byte, off int64) (n int, err error) {
	var data []byte
	idx := s.seek(off)
	if idx == len(s) {
		return 0, io.EOF
	}
	if s[idx].off > off {
		data = s.segOff(idx, off)
		n = copy(p, data)
		if n == len(p) {
			return n, nil
		}
		p = p[n:]
	}
	for {
		idx++
		if idx == len(s) {
			return n, io.EOF
		}
		data = s.seg(idx)
		c := copy(p, data)
		n += c
		if c == len(p) {
			return n, nil
		}
		p = p[c:]
	}
}
