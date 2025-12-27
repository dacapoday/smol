// Package overflow provides a method for reading and writing arbitrary-length data
// on top of fixed-size blocks. It uses a singly-linked list to chain multiple overflow
// pages when data exceeds a single block's capacity.
package overflow

import "encoding/binary"

// Head decodes the overflow header and returns the data head, size of data overflowed to other blocks, and next block ID.
func Head(head []byte) (front []byte, overflowSize int, overflowID BlockID) {
	size, slen := binary.Uvarint(head)
	if slen <= 0 {
		return
	}
	front = head[slen+4:]
	overflowSize = int(size)
	overflowID = binary.LittleEndian.Uint32(head[slen:])
	return
}

// Read reads the complete data from block by head.
// If buf has enough capacity, it will be reused;
// otherwise, a new buffer will be allocated.
// Returns the complete data in body.
func Read[B ReadOnly](block B, buf []byte, head []byte) (body []byte, err error) {
	front, overflowSize, overflowID := Head(head)
	if overflowID < 2 {
		err = ErrInvalidOverflowHead
		return
	}

	if size := len(front) + overflowSize; cap(buf) < size {
		buf = make([]byte, 0, size)
	} else {
		buf = buf[:0]
	}
	buf = append(buf, front...)

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
			err = ErrInvalidOverflowPage
			return
		}
	}

	for {
		if err = block.ReadBlock(overflowID, buffer, read); err != nil {
			return
		}
		if overflowID < 2 {
			return
		}
	}
}

// Recycle frees the overflow blocks by head.
func Recycle[B ReadWrite](block B, head []byte) (err error) {
	_, _, overflowID := Head(head)

	buffer := block.AllocateBuffer()
	defer block.RecycleBuffer(buffer)

	recycle := func(block []byte) {
		page := Page(block)
		if page.IsOverflowTail() {
			overflowID = 0
			return
		}
		overflowID = page.OverflowID()
		if overflowID < 2 {
			err = ErrInvalidOverflowPage
			return
		}
	}

	for {
		if overflowID < 2 {
			return
		}
		block.RecycleBlock(overflowID)

		if err = block.ReadBlock(overflowID, buffer, recycle); err != nil {
			return
		}
	}
}

// Write writes body to blocks and returns the encoded head.
// The inlineSize parameter specifies how many bytes to keep in head; the rest overflows to blocks.
func Write[B ReadWrite](block B, body []byte, inlineSize int) (head []byte, err error) {
	// inlineSize = min(max(0, inlineSize), len(body))
	front := body[:inlineSize]
	rest := body[inlineSize:]
	overflowSize := len(rest)
	bodySize := block.PageSize() - HeadSize - 4
	tailSize := overflowSize % bodySize
	if tailSize <= 4 && overflowSize > tailSize {
		tailSize += bodySize
	}

	buffer := block.AllocateBuffer()
	defer block.RecycleBuffer(buffer)

	beg := overflowSize - tailSize
	encodeTailPage(buffer, rest[beg:])

	overflowID := block.AllocateBlock()
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

	head = make([]byte, sizeUvarint(overflowSize)+4+len(front))
	n := binary.PutUvarint(head, uint64(overflowSize))
	binary.LittleEndian.PutUint32(head[n:], overflowID)
	copy(head[n+4:], front)
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
