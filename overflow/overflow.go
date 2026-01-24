// Package overflow implements arbitrary-length data storage on fixed-size blocks.
// Uses a singly-linked list to chain overflow pages when data exceeds block capacity.
package overflow

import (
	"bytes"
	"encoding/binary"
)

// Read reads complete data from block using head.
// Reuses buffer if capacity suffices; otherwise allocates.
func Read[B ReadOnly](block B, buffer []byte, head []byte, overflowSize int, overflowID BlockID) (body []byte, err error) {
	if size := len(head) + overflowSize; cap(buffer) < size {
		buffer = make([]byte, 0, size)
	}
	buffer = append(buffer[:0], head...)

	var chunk []byte
	for chunk, err = range Iter(block, overflowSize, overflowID) {
		if err != nil {
			return
		}
		buffer = append(buffer, chunk...)
	}
	body = buffer
	return
}

// Compare compares key with overflow data (head + overflow chain).
// Returns -1 (key < data), 0 (key == data), 1 (key > data).
func Compare[B ReadOnly](block B, key, head []byte, overflowSize int, overflowID BlockID) (cmp int, err error) {
	// compare head prefix
	n := min(len(key), len(head))
	if cmp = bytes.Compare(key[:n], head[:n]); cmp != 0 {
		return
	}

	// key within head
	size := len(head) + overflowSize
	if len(key) <= len(head) {
		if len(key) < size {
			cmp = -1
		}
		return
	}

	// compare overflow
	offset := len(head)
	var chunk []byte
	for chunk, err = range Iter(block, overflowSize, overflowID) {
		if err != nil {
			return
		}
		remaining := len(key) - offset
		n := min(remaining, len(chunk))
		if cmp = bytes.Compare(key[offset:offset+n], chunk[:n]); cmp != 0 {
			return
		}
		if remaining < len(chunk) {
			cmp = -1
			return
		}
		offset += len(chunk)
	}

	if len(key) > size {
		cmp = 1
	}
	return
}

// Iter returns an iterator over each page's data in the overflow chain.
// Data is only valid during the yield call.
// Reports ErrBadOverflow if data size doesn't match overflowSize.
func Iter[B ReadOnly](block B, overflowSize int, overflowID BlockID) func(yield func([]byte, error) bool) {
	return func(yield func([]byte, error) bool) {
		buffer := block.AllocateBuffer()
		defer block.RecycleBuffer(buffer)

		read := func(buffer []byte) {
			page := Page(buffer)
			var data []byte
			if page.IsOverflowTail() {
				data = page.OverflowTail()
				overflowID = 0
			} else {
				data = page.OverflowBody()
				overflowID = page.OverflowID()
			}
			overflowSize -= len(data)
			if overflowSize < 0 {
				yield(nil, errOverflow(overflowSize))
				overflowID = 0
				overflowSize = 0
				return
			}
			if !yield(data, nil) {
				overflowID = 0
				overflowSize = 0
			}
		}

		for overflowID > 1 {
			if err := block.ReadBlock(overflowID, buffer, read); err != nil {
				yield(nil, err)
				return
			}
		}

		if overflowSize != 0 {
			yield(nil, errOverflow(overflowSize))
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
			err = errNextID(nextID)
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
