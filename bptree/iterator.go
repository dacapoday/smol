// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package bptree

import (
	"bytes"

	"github.com/dacapoday/smol/iterator"
	"github.com/dacapoday/smol/overflow"
)

var _ iterator.Iterator = (*Reader[ReadOnly])(nil)

// Valid returns true if positioned at a valid item.
func (reader *Reader[B]) Valid() bool {
	return reader.err == null
}

// Error returns any error encountered during iteration.
func (reader *Reader[B]) Error() error {
	if reader.err == nil {
		return ErrClosed
	}
	if reader.err == null || reader.err == exhausted {
		return nil
	}
	return reader.err
}

// Key returns the current key, or nil if invalid.
//
// Warning: Returned slice is valid only until next method call.
func (reader *Reader[B]) Key() (key []byte) {
	if reader.err != null {
		return
	}
	key = reader.page.LeafKey(reader.index)
	keyInlineSize := int(reader.keyInlineSize)
	if len(key) > keyInlineSize {
		head, overflowSize, overflowID := Overflow(key, keyInlineSize)
		var err error
		key, err = overflow.Read(reader.block, nil, head, overflowSize, overflowID)
		if err != nil {
			reader.err = err
		}
	}
	return
}

// Val returns the current value, or nil if invalid or deleted.
//
// Warning: Returned slice is valid only until next method call.
func (reader *Reader[B]) Val() (val []byte) {
	if reader.err != null {
		return
	}
	val = reader.page.LeafVal(reader.index)
	valInlineSize := int(reader.valInlineSize)
	if len(val) > valInlineSize {
		if len(reader.val) != 0 {
			val = reader.val
			return
		}
		head, overflowSize, overflowID := Overflow(val, valInlineSize)
		var err error
		val, err = overflow.Read(reader.block, reader.val, head, overflowSize, overflowID)
		if err != nil {
			reader.err = err
			return
		}
		reader.val = val
	}
	return
}

// Next advances to the next item.
func (reader *Reader[B]) Next() bool {
	if reader.err != null {
		return false
	}
	if reader.next() {
		reader.val = reader.val[:0]
		return true
	}
	high := len(reader.level)
	h := high - 1
	for {
		if h < 0 {
			reader.err = exhausted
			return false
		}
		if reader.level.next(h) {
			break
		}
		h--
	}
	var blockID BlockID
	if h == 0 {
		blockID = reader.root.BranchID(reader.level[0].Index)
	} else if err := reader.block.ReadBlock(reader.level[h].BlockID, reader.page, func(block []byte) {
		blockID = Page(block).BranchID(reader.level[h].Index)
	}); err != nil {
		reader.err = err
		return false
	}
	seekFirst := func(block []byte) {
		page := Page(block)
		count := page.Count()
		reader.level[h].BlockID = blockID
		reader.level[h].Count = count
		reader.level[h].Index = 0
		blockID = page.BranchID(0)
	}
	for h++; h < high; h++ {
		if err := reader.block.ReadBlock(blockID, reader.page, seekFirst); err != nil {
			reader.err = err
			return false
		}
	}
	if err := reader.block.ReadBlock(blockID, reader.page, nil); err != nil {
		reader.err = err
		return false
	}
	count := reader.page.Count()
	reader.level[0].BlockID = blockID
	reader.count = count
	reader.index = 0
	reader.val = reader.val[:0]
	return true
}

// Prev moves to the previous item.
func (reader *Reader[B]) Prev() bool {
	if reader.err != null {
		return false
	}
	if reader.prev() {
		reader.val = reader.val[:0]
		return true
	}
	high := len(reader.level)
	h := high - 1
	for {
		if h < 0 {
			reader.err = exhausted
			return false
		}
		if reader.level.prev(h) {
			break
		}
		h--
	}
	var blockID BlockID
	if h == 0 {
		blockID = reader.root.BranchID(reader.level[0].Index)
	} else if err := reader.block.ReadBlock(reader.level[h].BlockID, reader.page, func(block []byte) {
		blockID = Page(block).BranchID(reader.level[h].Index)
	}); err != nil {
		reader.err = err
		return false
	}
	seekLast := func(block []byte) {
		page := Page(block)
		count := page.Count()
		index := count - 1
		reader.level[h].BlockID = blockID
		reader.level[h].Count = count
		reader.level[h].Index = index
		blockID = page.BranchID(index)
	}
	for h++; h < high; h++ {
		if err := reader.block.ReadBlock(blockID, reader.page, seekLast); err != nil {
			reader.err = err
			return false
		}
	}
	if err := reader.block.ReadBlock(blockID, reader.page, nil); err != nil {
		reader.err = err
		return false
	}
	count := reader.page.Count()
	reader.level[0].BlockID = blockID
	reader.count = count
	reader.index = count - 1
	reader.val = reader.val[:0]
	return true
}

// SeekFirst positions at the first key.
func (reader *Reader[B]) SeekFirst() bool {
	// if reader.err != null {
	// 	return false
	// }
	high := len(reader.level)
	if high == 0 {
		if reader.level == nil {
			if reader.err == nil {
				return false
			}
			return reader.seekFirst()
		}
		count := reader.page.Count()
		if count == 0 {
			reader.err = exhausted
			return false
		}
		reader.count = count
		reader.index = 0
		reader.err = null
		reader.val = reader.val[:0]
		return true
	}
	page := reader.root
	count := page.Count()
	reader.level[0].Count = count
	reader.level[0].Index = 0
	blockID := page.BranchID(0)
	h := 1
	seekFirst := func(block []byte) {
		page := Page(block)
		count := page.Count()
		reader.level[h].BlockID = blockID
		reader.level[h].Count = count
		reader.level[h].Index = 0
		blockID = page.BranchID(0)
	}
	for ; h < high; h++ {
		reader.err = reader.block.ReadBlock(blockID, reader.page, seekFirst)
		if reader.err != nil {
			return false
		}
	}
	reader.err = reader.block.ReadBlock(blockID, reader.page, nil)
	if reader.err != nil {
		return false
	}
	count = reader.page.Count()
	reader.level[0].BlockID = blockID
	reader.count = count
	reader.index = 0
	reader.err = null
	reader.val = reader.val[:0]
	return true
}

// SeekLast positions at the last key.
func (reader *Reader[B]) SeekLast() bool {
	// if reader.err != null {
	// 	return false
	// }
	high := len(reader.level)
	if high == 0 {
		if reader.level == nil {
			if reader.err == nil {
				return false
			}
			return reader.seekLast()
		}
		count := reader.page.Count()
		if count == 0 {
			reader.err = exhausted
			return false
		}
		reader.count = count
		reader.index = count - 1
		reader.err = null
		reader.val = reader.val[:0]
		return true
	}
	page := reader.root
	count := page.Count()
	index := count - 1
	reader.level[0].Count = count
	reader.level[0].Index = index
	blockID := page.BranchID(index)
	h := 1
	seekLast := func(block []byte) {
		page := Page(block)
		count := page.Count()
		index := count - 1
		reader.level[h].BlockID = blockID
		reader.level[h].Count = count
		reader.level[h].Index = index
		blockID = page.BranchID(index)
	}
	for ; h < high; h++ {
		reader.err = reader.block.ReadBlock(blockID, reader.page, seekLast)
		if reader.err != nil {
			return false
		}
	}
	reader.err = reader.block.ReadBlock(blockID, reader.page, nil)
	if reader.err != nil {
		return false
	}
	count = reader.page.Count()
	index = count - 1
	reader.level[0].BlockID = blockID
	reader.count = count
	reader.index = index
	reader.err = null
	reader.val = reader.val[:0]
	return true
}

// Seek positions at the first key >= the given key.
func (reader *Reader[B]) Seek(key []byte) bool {
	// if reader.err != null {
	// 	return false
	// }
	cursor := cursor[B]{
		Reader:        reader,
		key:           key,
		keyInlineSize: int(reader.keyInlineSize),
	}
	high := len(reader.level)
	if high == 0 {
		if reader.level == nil {
			if reader.err == nil {
				return false
			}
			return reader.seek(key)
		}
		page := reader.page
		count := page.Count()
		if count == 0 {
			reader.err = exhausted
			return false
		}
		index := cursor.searchLeaf(count, page)
		if cursor.err != nil {
			reader.err = cursor.err
			return false
		}
		if index == count {
			reader.err = exhausted
			return false
		}
		reader.count = count
		reader.index = index
		reader.err = null
		reader.val = reader.val[:0]
		return true
	}
	page := reader.root
	count := page.Count()
	index := cursor.searchBranch(count, page)
	if cursor.err != nil {
		reader.err = cursor.err
		return false
	}
	if index == count {
		reader.err = exhausted
		return false
	}
	reader.level[0].Count = count
	reader.level[0].Index = index
	blockID := page.BranchID(index)
	h := 1
	seek := func(block []byte) {
		page := Page(block)
		count := page.Count()
		index := cursor.searchBranch(count-1, page)
		reader.level[h].BlockID = blockID
		reader.level[h].Count = count
		reader.level[h].Index = index
		blockID = page.BranchID(index)
	}
	for ; h < high; h++ {
		reader.err = reader.block.ReadBlock(blockID, reader.page, seek)
		if reader.err != nil {
			return false
		}
		if cursor.err != nil {
			reader.err = cursor.err
			return false
		}
	}
	reader.err = reader.block.ReadBlock(blockID, reader.page, nil)
	if reader.err != nil {
		return false
	}
	page = reader.page
	count = page.Count()
	index = cursor.searchLeaf(count-1, page)
	if cursor.err != nil {
		reader.err = cursor.err
		return false
	}
	reader.level[0].BlockID = blockID
	reader.count = count
	reader.index = index
	reader.err = null
	reader.val = reader.val[:0]
	return true
}

// cursor should stack-only; no escape
type cursor[B ReadOnly] struct {
	*Reader[B]
	key           []byte
	page          Page
	err           error
	keyInlineSize int
}

func (cursor *cursor[B]) searchLeaf(count uint16, page Page) (index uint16) {
	cursor.page = page
	index = search(count, cursor.leaf)
	// cursor.page = nil
	return
}

func (cursor *cursor[B]) leaf(i uint16) int {
	inlineKey := cursor.page.LeafKey(i)
	if len(inlineKey) <= cursor.keyInlineSize {
		return bytes.Compare(cursor.key, inlineKey)
	}
	return cursor.compare(inlineKey)
}

func (cursor *cursor[B]) searchBranch(count uint16, page Page) (index uint16) {
	cursor.page = page
	index = search(count, cursor.branch)
	// cursor.page = nil
	return
}

func (cursor *cursor[B]) branch(i uint16) int {
	inlineKey := cursor.page.BranchKey(i)
	if len(inlineKey) <= cursor.keyInlineSize {
		return bytes.Compare(cursor.key, inlineKey)
	}
	return cursor.compare(inlineKey)
}

func (cursor *cursor[B]) compare(inlineKey []byte) int {
	if cursor.err != nil {
		return 0
	}
	head, overflowSize, overflowID := Overflow(inlineKey, cursor.keyInlineSize)
	cmp, err := overflow.Compare(cursor.block, cursor.key, head, overflowSize, overflowID)
	if err != nil {
		cursor.err = err
		return 0
	}
	return cmp
}
