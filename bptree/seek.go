// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package bptree

import (
	"bytes"

	"github.com/dacapoday/smol/overflow"
)

func (reader *Reader[B]) seekFirst() bool {
	page := reader.root
	reader.level = Level{{
		Count: page.Count(),
		Index: 0,
	}}
	blockID := page.BranchID(0)
	seekFirst := func(block []byte) {
		page := Page(block)
		if page.IsLeaf() {
			if &page[0] != &reader.page[0] {
				copy(reader.page, page)
			}
			reader.count = page.Count()
			reader.index = 0
			reader.level[0].BlockID = blockID
			blockID = 0
			return
		}
		reader.level = append(reader.level, level{
			BlockID: blockID,
			Count:   page.Count(),
			Index:   0,
		})
		blockID = page.BranchID(0)
	}
	page = reader.page
	for blockID > 1 {
		reader.err = reader.block.ReadBlock(blockID, page, seekFirst)
		if reader.err != nil {
			return false
		}
	}
	reader.err = null
	reader.val = reader.val[:0]
	return true
}

func (reader *Reader[B]) seekLast() bool {
	page := reader.root
	count := page.Count()
	index := count - 1
	reader.level = Level{{
		Count: count,
		Index: index,
	}}
	blockID := page.BranchID(index)
	seekLast := func(block []byte) {
		page := Page(block)
		count := page.Count()
		index := count - 1
		if page.IsLeaf() {
			if &page[0] != &reader.page[0] {
				copy(reader.page, page)
			}
			reader.count = count
			reader.index = index
			reader.level[0].BlockID = blockID
			blockID = 0
			return
		}
		reader.level = append(reader.level, level{
			BlockID: blockID,
			Count:   count,
			Index:   index,
		})
		blockID = page.BranchID(index)
	}
	page = reader.page
	for blockID > 1 {
		reader.err = reader.block.ReadBlock(blockID, page, seekLast)
		if reader.err != nil {
			return false
		}
	}
	reader.err = null
	reader.val = reader.val[:0]
	return true
}

func (reader *Reader[B]) seek(key []byte) bool {
	cursor := cursor[B]{
		Reader:        reader,
		key:           key,
		keyInlineSize: int(reader.keyInlineSize),
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
	reader.level = Level{{
		Count: count,
		Index: index,
	}}
	blockID := page.BranchID(index)
	seek := func(block []byte) {
		page := Page(block)
		count := page.Count()
		if page.IsLeaf() {
			if &page[0] != &reader.page[0] {
				copy(reader.page, page)
			}
			index := cursor.searchLeaf(count-1, page)
			reader.count = count
			reader.index = index
			reader.level[0].BlockID = blockID
			blockID = 0
			return
		}
		index := cursor.searchBranch(count-1, page)
		reader.level = append(reader.level, level{
			BlockID: blockID,
			Count:   count,
			Index:   index,
		})
		blockID = page.BranchID(index)
	}
	page = reader.page
	for blockID > 1 {
		reader.err = reader.block.ReadBlock(blockID, page, seek)
		if reader.err != nil {
			return false
		}
		if cursor.err != nil {
			reader.err = cursor.err
			return false
		}
	}
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
