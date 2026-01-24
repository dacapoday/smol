// Package overflow implements arbitrary-length data storage on fixed-size blocks.
// Uses a singly-linked list to chain overflow pages when data exceeds block capacity.
package overflow

import (
	"encoding/binary"
)

// Read reads complete data from block using head.
// Reuses buf if it has enough capacity; otherwise allocates new buffer.
// Returns complete data in body.
func Read[B ReadOnly](block B, buf []byte, head []byte, overflowSize int, overflowID BlockID) (body []byte, err error) {
	if overflowID < 2 {
		err = ErrBadOverflow
		return
	}

	if size := len(head) + overflowSize; cap(buf) < size {
		buf = make([]byte, 0, size)
	} else {
		buf = buf[:0]
	}
	buf = append(buf, head...)

	buffer := block.AllocateBuffer()
	defer block.RecycleBuffer(buffer)

	read := func(block []byte) {
		page := Page(block)
		if page.IsOverflowTail() {
			body = append(buf, page.OverflowTail()...)
			overflowID = 0
			return
		}
		buf = append(buf, page.OverflowBody()...)
		overflowID = page.OverflowID()
		if overflowID < 2 {
			overflowID = 1
			return
		}
	}

	for {
		if err = block.ReadBlock(overflowID, buffer, read); err != nil {
			return
		}
		if overflowID < 2 {
			if overflowID == 1 {
				err = ErrBadOverflow
			}
			return
		}
	}
}

// Iter returns an iterator over each page's data in the overflow chain.
// Data is only valid during the yield call.
func Iter[B ReadOnly](block B, overflowID BlockID) func(yield func([]byte, error) bool) {
	return func(yield func([]byte, error) bool) {
		buffer := block.AllocateBuffer()
		defer block.RecycleBuffer(buffer)

		read := func(buffer []byte) {
			page := Page(buffer)
			if page.IsOverflowTail() {
				yield(page.OverflowTail(), nil)
				overflowID = 0
				return
			}
			if !yield(page.OverflowBody(), nil) {
				overflowID = 0
				return
			}
			overflowID = page.OverflowID()
		}

		for overflowID > 1 {
			if err := block.ReadBlock(overflowID, buffer, read); err != nil {
				yield(nil, err)
				return
			}
		}
	}
}

// Recycle frees overflow blocks using overflowID.
func Recycle[B ReadWrite](block B, overflowID BlockID) (err error) {
	buffer := block.AllocateBuffer()
	defer block.RecycleBuffer(buffer)

	var nextID BlockID
	recycle := func(buf []byte) {
		page := Page(buf)
		if page.IsOverflowTail() {
			nextID = 0
			return
		}

		nextID = page.OverflowID()
		if nextID < 2 {
			err = ErrBadOverflow
		}
	}

	for overflowID > 1 {
		if e := block.ReadBlock(overflowID, buffer, recycle); e != nil {
			err = e
			return
		}
		block.RecycleBlock(overflowID)
		overflowID = nextID
	}
	return
}

// Write writes body to blocks and returns the encoded head.
// The inlineSize parameter specifies how many bytes to keep in head; the rest overflows to blocks.
func Write[B ReadWrite](block B, body []byte, inlineSize int) (head []byte, overflowSize int, overflowID BlockID, err error) {
	head = body[:inlineSize]
	rest := body[inlineSize:]
	overflowSize = len(rest)
	if overflowSize == 0 {
		return
	}

	bodySize := block.PageSize() - HeadSize - 4
	tailSize := overflowSize % bodySize
	if tailSize <= 4 && overflowSize > tailSize {
		tailSize += bodySize
	}

	buffer := block.AllocateBuffer()
	defer block.RecycleBuffer(buffer)

	beg := overflowSize - tailSize
	encodeTailPage(buffer, rest[beg:])

	overflowID = block.AllocateBlock()
	if overflowID < 2 {
		err = errAllocateFailed(block)
		return
	}

	if err = block.WriteBlock(overflowID, buffer); err != nil {
		return
	}

	for beg != 0 {
		end := beg
		beg -= bodySize
		encodeBodyPage(buffer, rest[beg:end], overflowID)

		overflowID = block.AllocateBlock()
		if overflowID < 2 {
			err = errAllocateFailed(block)
			return
		}

		if err = block.WriteBlock(overflowID, buffer); err != nil {
			return
		}
	}
	return
}

func encodeTailPage(buffer []byte, body []byte) {
	copy(buffer[HeadSize:], body)
	binary.LittleEndian.PutUint16(buffer[2:], uint16(len(body)))
	buffer[1] = 0
	buffer[0] = 0
}

func encodeBodyPage(buffer []byte, body []byte, overflowID BlockID) {
	copy(buffer[HeadSize+4:], body)
	binary.LittleEndian.PutUint32(buffer[HeadSize:], overflowID)
	binary.LittleEndian.PutUint16(buffer[2:], uint16(4+len(body)))
	buffer[1] = 0x40
	buffer[0] = 0
}
