package overflow

import "encoding/binary"

// Page represents a overflow page.
type Page []byte

const HeadSize = 4 // Head + Size

// Size returns the total size of the page in bytes.
func (page Page) Size() int {
	if len(page) < HeadSize {
		return len(page)
	}
	return int(binary.LittleEndian.Uint16(page[2:])) + HeadSize
}

// IsOverflowTail reports whether this page is the last page in the overflow chain.
func (page Page) IsOverflowTail() bool {
	if len(page) < HeadSize {
		return true
	}
	return page[1]&0x40 == 0
}

// OverflowBody returns the data content of a non-tail overflow page.
// Only call this method on non-tail pages (when IsOverflowTail returns false).
func (page Page) OverflowBody() []byte {
	return page[HeadSize+4 : page.Size()]
}

// OverflowID returns the block ID of the next overflow page in the chain.
// Only call this method on tail pages (when IsOverflowTail returns true).
func (page Page) OverflowID() BlockID {
	return binary.LittleEndian.Uint32(page[HeadSize:])
}

// OverflowTail returns the data content of a tail overflow page.
// Only call this method on tail pages (when IsOverflowTail returns true).
func (page Page) OverflowTail() []byte {
	return page[HeadSize:page.Size()]
}

func sizeUvarint(x int) (size int) {
	switch {
	case x < 128: // 1<<7
		return 1
	case x < 16384: // 1<<14
		return 2
	case x < 2097152: // 1<<21
		return 3
	case x < 268435456: // 1<<28
		return 4
	case x < 34359738368: // 1<<35
		return 5
	case x < 4398046511104: // 1<<42
		return 6
	case x < 562949953421312: // 1<<49
		return 7
	case x < 72057594037927936: // 1<<56
		return 8
	default:
		return 9
	}
}

// func sizeUvarint(x uint64) (size uint64) {
// 	for {
// 		size++
// 		x >>= 7
// 		if x == 0 {
// 			return
// 		}
// 	}
// }
