// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package bptree

import "github.com/dacapoday/smol/overflow"

// Reader provides cursor-based traversal of B+ tree items.
type Reader[B ReadOnly] struct {
	block         B
	root          Page
	err           error
	level         Level
	page          Page   // buf
	val           []byte // buf
	count         uint16
	index         uint16
	keyInlineSize uint16
	valInlineSize uint16
}

func (reader *Reader[B]) Block() B {
	return reader.block
}

// Load initializes the reader with block and root page.
// Positions reader before the first entry.
func (reader *Reader[B]) Load(block B, root Page, keyInlineSize, valInlineSize int, high uint8) {
	reader.block = block
	reader.root = root
	reader.keyInlineSize = uint16(keyInlineSize)
	reader.valInlineSize = uint16(valInlineSize)
	reader.err = exhausted
	// if len(reader.level) != 0 {
	// 	reader.block.RecycleBuffer(reader.page)
	// }
	if high == 0 {
		if root.IsLeaf() {
			reader.level = Level{}
			reader.page = root
		} else {
			reader.level = nil
			reader.page = block.AllocateBuffer()
		}
	} else {
		reader.level = make(Level, high)
		reader.page = block.AllocateBuffer()
	}
	reader.count = 0
	reader.index = 0
}

// LoadFrom initializes the reader by copying state from src.
// Creates independent copy at the same position.
func (dst *Reader[B]) LoadFrom(src *Reader[B]) {
	dst.block = src.block
	dst.root = src.root
	dst.keyInlineSize = src.keyInlineSize
	dst.valInlineSize = src.valInlineSize
	dst.err = src.err
	dst.count = src.count
	dst.index = src.index
	dst.val = nil
	if dst.err == nil {
		dst.level = nil
		dst.page = nil
		return
	}
	if len(src.level) == 0 {
		if src.level == nil {
			dst.level = nil
			dst.page = src.block.AllocateBuffer()
		} else {
			dst.level = Level{}
			dst.page = src.page
		}
		return
	}
	dst.level = append(Level(nil), src.level...)
	dst.page = src.block.AllocateBuffer()
	if dst.err != null {
		return
	}
	if err := dst.block.ReadBlock(dst.level[0].BlockID, dst.page, nil); err != nil {
		dst.err = err
	}
}

// Close releases resources and resets the reader.
func (reader *Reader[B]) Close() {
	if len(reader.level) != 0 {
		reader.block.RecycleBuffer(reader.page)
	} else if reader.level == nil && reader.page != nil {
		reader.block.RecycleBuffer(reader.page)
	}

	reader.err = nil
	reader.level = nil
	reader.root = nil
	reader.page = nil
	reader.val = nil
	reader.count = 0
	reader.index = 0
	reader.keyInlineSize = 0
	reader.valInlineSize = 0

	var nilBlock B
	reader.block = nilBlock
}

// Level returns a copy of the current path from root to leaf, representing the
// cursor position in the tree.
func (reader *Reader[B]) Level() Level {
	return append(Level(nil), reader.level...)
}

func (reader *Reader[B]) next() bool {
	reader.index++
	if reader.index < reader.count {
		return true
	}
	reader.index--
	return false
}

func (reader *Reader[B]) prev() bool {
	if reader.index == 0 {
		return false
	}
	reader.index--
	return true
}

// KeyStr returns the current key as string, or empty if invalid.
func (reader *Reader[B]) KeyStr() string {
	return b2s(reader.Key())
}

// ValStr returns the current value as string, or empty if invalid.
func (reader *Reader[B]) ValStr() string {
	return b2s(reader.Val())
}

// InlineKey returns the key bytes stored directly in the page slot.
func (reader *Reader[B]) InlineKey() (key []byte) {
	if reader.err != null {
		return
	}
	key = reader.page.LeafKey(reader.index)
	return
}

// InlineVal returns the value bytes stored directly in the page slot.
func (reader *Reader[B]) InlineVal() (val []byte) {
	if reader.err != null {
		return
	}
	val = reader.page.LeafVal(reader.index)
	return
}

// InlineKeyStr returns the inline key as string.
func (reader *Reader[B]) InlineKeyStr() string {
	return b2s(reader.InlineKey())
}

// InlineValStr returns the inline value as string.
func (reader *Reader[B]) InlineValStr() string {
	return b2s(reader.InlineVal())
}

func (reader *Reader[B]) KeyCopy(buf []byte) (key []byte) {
	if reader.err != null {
		return
	}
	k := reader.page.LeafKey(reader.index)
	keyInlineSize := int(reader.keyInlineSize)
	if len(k) > keyInlineSize {
		head, overflowSize, overflowID := Overflow(k, keyInlineSize)
		var err error
		key, err = overflow.Read(reader.block, buf, head, overflowSize, overflowID)
		if err != nil {
			reader.err = err
		}
		return
	}
	if key = append(buf[:0], k...); key == nil {
		key = []byte{}
	}
	return
}

func (reader *Reader[B]) ValCopy(buf []byte) (val []byte) {
	if reader.err != null {
		return
	}
	v := reader.page.LeafVal(reader.index)
	valInlineSize := int(reader.valInlineSize)
	if len(v) > valInlineSize {
		if len(reader.val) != 0 {
			val = append(buf[:0], reader.val...)
			return
		}
		head, overflowSize, overflowID := Overflow(v, valInlineSize)
		var err error
		val, err = overflow.Read(reader.block, buf, head, overflowSize, overflowID)
		if err != nil {
			reader.err = err
		}
		return
	}
	if val = append(buf[:0], v...); val == nil {
		val = []byte{}
	}
	return
}
