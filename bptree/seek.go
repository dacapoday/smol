// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package bptree

func (reader *Reader[B]) seekFirst() bool {
	page := reader.root
	reader.level = Level{{
		Count: page.Count(),
		Index: 0,
	}}
	blockID := page.BranchID(0)
	page = reader.page
	for {
		reader.err = reader.block.ReadBlock(blockID, page, nil)
		if reader.err != nil {
			return false
		}
		if page.IsLeaf() {
			break
		}
		reader.level = append(reader.level, level{
			BlockID: blockID,
			Count:   page.Count(),
			Index:   0,
		})
		blockID = page.BranchID(0)
	}
	reader.level[0].BlockID = blockID
	reader.count = page.Count()
	reader.index = 0
	reader.err = null
	reader.key = reader.key[:0]
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
	page = reader.page
	for {
		reader.err = reader.block.ReadBlock(blockID, page, nil)
		if reader.err != nil {
			return false
		}
		if page.IsLeaf() {
			break
		}
		count = page.Count()
		index = count - 1
		reader.level = append(reader.level, level{
			BlockID: blockID,
			Count:   count,
			Index:   index,
		})
		blockID = page.BranchID(index)
	}
	count = page.Count()
	index = count - 1
	reader.level[0].BlockID = blockID
	reader.count = count
	reader.index = index
	reader.err = null
	reader.key = reader.key[:0]
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
	page = reader.page
	for {
		reader.err = reader.block.ReadBlock(blockID, page, nil)
		if reader.err != nil {
			return false
		}
		if page.IsLeaf() {
			break
		}
		count = page.Count()
		index = cursor.searchBranch(count-1, page)
		if cursor.err != nil {
			reader.err = cursor.err
			return false
		}
		reader.level = append(reader.level, level{
			BlockID: blockID,
			Count:   count,
			Index:   index,
		})
		blockID = page.BranchID(index)
	}
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
	reader.key = reader.key[:0]
	reader.val = reader.val[:0]
	return true
}
