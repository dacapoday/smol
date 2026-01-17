// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package bptree

import (
	"bytes"
	"errors"

	"github.com/dacapoday/smol/overflow"
)

// WriteSortedChanges applies a batch of changes to the copy-on-write B+ tree.
// It returns a new root page and tree height without modifying the original tree.
//
// sortedChanges must yield key-value pairs in ascending lexicographic order.
// A nil value indicates deletion of the key. All yielded keys and values must
// remain valid until the function returns, not just during iteration.
func WriteSortedChanges[B ReadWrite, R RootBlock](block B, root R, sortedChanges func(func([]byte, []byte) bool)) (high uint8, page Page, err error) {
	writer := itemWriter[B]{block: block}
	writer.keyInlineSize = root.KeyInlineSize()
	writer.valInlineSize = root.ValInlineSize()
	writer.root.high = root.High()
	writer.root.page = root.Page()
	writer.list.head = new(leafNode)
	writer.list.tail = writer.list.head

	if writer.root.high == 0 {
		writer.list.tail.Page = writer.root.page
		writer.list.tail.page.tail.beg = 0
		writer.list.tail.page.tail.end = writer.list.tail.page.Count()
		if writer.list.tail.page.tail.end == 0 {
			if pageSize := writer.block.PageSize(); pageSize < 512 {
				panic(errors.New("pageSize < 512"))
			} else if pageSize > 65536 {
				panic(errors.New("pageSize > 65536"))
			} else {
				maxKeyInlineSize := maxKeyInlineSize(pageSize)
				if writer.keyInlineSize > maxKeyInlineSize {
					panic(errors.New("keyInlineSize > maxKeyInlineSize"))
				}
				maxValInlineSize := maxValInlineSize(pageSize, writer.keyInlineSize)
				if writer.valInlineSize > maxValInlineSize {
					panic(errors.New("valInlineSize > maxValInlineSize"))
				}
			}
		}
		for key, val := range sortedChanges {
			writer.find(key)
			if writer.err != nil {
				return 0, nil, writer.err
			}
			writer.write(key, val)
		}
	} else {
		writer.pages = make(pages)
		defer recyclePages(block, writer.pages)
		for key, val := range sortedChanges {
			if writer.list.tail.page.tail.beg == writer.list.tail.page.tail.end {
				if writer.last {
					writer.write(key, val)
					continue
				}
				writer.seek(key) // first
				if writer.err != nil {
					return 0, nil, writer.err
				}
			}
			writer.find(key)
			if writer.err != nil {
				return 0, nil, writer.err
			}
			if !writer.last && writer.index == writer.list.tail.page.tail.end {
				if block.BufferPressured(writer.loaded()) {
					writer.flush()
					if writer.err != nil {
						return 0, nil, writer.err
					}
				}
				writer.seek(key)
				if writer.err != nil {
					return 0, nil, writer.err
				}
				writer.find(key)
				if writer.err != nil {
					return 0, nil, writer.err
				}
			}
			writer.write(key, val)
		}
		writer.list.head = writer.list.head.next
		if writer.list.trunk() {
			recycleTrunk(block, writer.list.head)
		}
	}

	if err := writer.wait(); err != nil {
		return 0, nil, err
	}

	if o, ok := any(root).(MakePage); ok {
		return writeBranch(
			block,
			o.MakePage,
			writer.root.high, writer.root.page,
			writer.pages, writer.list.head,
		)
	}

	return writeBranch(
		block,
		makePage,
		writer.root.high, writer.root.page,
		writer.pages, writer.list.head,
	)
}

// itemWriter should stack-only; no escape
type itemWriter[B ReadWrite] struct {
	block B
	task
	root struct {
		high uint8
		page Page
	}
	keyInlineSize int
	valInlineSize int

	pages
	list[[]byte, LeafItems, leafItem, *leafItem]
	tail  *leafItem
	index uint16
	found bool
	last  bool
	err   error

	key []byte
	buf []byte
}

func (writer *itemWriter[B]) loaded() int {
	return len(writer.pages) + int(writer.root.high)
}

func makePage(size int) []byte { return make([]byte, size) }

func (writer *itemWriter[B]) flush() {
	writer.err = writer.wait()
	if writer.err != nil {
		return
	}

	writer.root.high, writer.root.page, writer.err = writeBranch(
		writer.block,
		makePage,
		writer.root.high, writer.root.page,
		writer.pages, writer.list.head.next,
	)
	if writer.err != nil {
		return
	}

	recyclePages(writer.block, writer.pages)
	writer.list.head.next = nil
	writer.list.tail = writer.list.head
	writer.tail = nil
	writer.index = 0
	writer.found = false
	writer.last = false
	writer.key = nil
}

func (writer *itemWriter[B]) load(blockID BlockID) (page Page) {
	page, ok := writer.pages[blockID]
	if ok {
		return
	}

	page, writer.err = writer.block.LoadBlock(blockID)
	if writer.err == nil {
		writer.pages[blockID] = page
	}
	return
}

func (writer *itemWriter[B]) seek(key []byte) {
	writer.key = key
	prev := writer.list.tail
	writer.list.tail = new(leafNode)
	writer.list.tail.level = make(Level, writer.root.high)
	// if len(writer.list.tail.level) == 0 {
	// 	panic(errors.New("len(writer.list.tail.level) == 0"))
	// }
	page := writer.root.page
	count := page.Count()
	if count == 0 {
		panic(errors.New("count == 0"))
	}
	writer.list.tail.Page = page
	index := search(count, writer.branch)
	// writer.list.tail.Page = nil
	if writer.err != nil {
		return
	}
	if index == count {
		if writer.last {
			panic(errors.New("writer.last && index == count"))
		}
		writer.last = true
		if prev != writer.list.head && prev.level.last() {
			writer.list.tail = prev
			return
		}
		index--
	}
	prev.next = writer.list.tail
	writer.list.tail.level[0].Count = count
	writer.list.tail.level[0].Index = index
	blockID := page.BranchID(index)
	high := len(writer.list.tail.level)
	for i := 1; i < high; i++ {
		page = writer.load(blockID)
		if writer.err != nil {
			return
		}
		count := page.Count()
		writer.list.tail.Page = page
		index := search(count-1, writer.branch)
		// writer.list.tail.Page = nil
		if writer.err != nil {
			return
		}
		writer.list.tail.level[i].BlockID = blockID
		writer.list.tail.level[i].Count = count
		writer.list.tail.level[i].Index = index
		blockID = page.BranchID(index)
	}
	page = writer.load(blockID)
	if writer.err != nil {
		return
	}
	writer.list.tail.Page = page
	writer.list.tail.page.tail.beg = 0
	writer.list.tail.page.tail.end = page.Count()
	writer.list.tail.level[0].BlockID = blockID
	writer.tail = nil
	writer.block.RecycleBlock(blockID)
}

func (writer *itemWriter[B]) branch(i uint16) int {
	branchKey := writer.list.tail.page.BranchKey(i)
	if len(branchKey) > writer.keyInlineSize {
		if writer.err != nil {
			return 0
		}
		branchKey, writer.err = overflow.Read(writer.block, writer.buf, branchKey)
		if writer.err != nil {
			return 0
		}
		writer.buf = branchKey
	}
	return bytes.Compare(writer.key, branchKey)
}

func (writer *itemWriter[B]) find(key []byte) {
	writer.key = key
	writer.index, writer.found = find(writer.list.tail.page.tail.end-writer.list.tail.page.tail.beg, writer.leaf)
	writer.index += writer.list.tail.page.tail.beg
}

func (writer *itemWriter[B]) leaf(i uint16) int {
	leafKey := writer.list.tail.page.LeafKey(writer.list.tail.page.tail.beg + i)
	if len(leafKey) > writer.keyInlineSize {
		if writer.err != nil {
			return 0
		}
		leafKey, writer.err = overflow.Read(writer.block, writer.buf, leafKey)
		if writer.err != nil {
			return 0
		}
		writer.buf = leafKey
	}
	return bytes.Compare(writer.key, leafKey)
}

func (writer *itemWriter[B]) write(key, val []byte) {
	block := writer.block
	if val != nil {
		if writer.found {
			// update
			if leafVal := writer.list.tail.page.LeafVal(writer.index); len(leafVal) > writer.valInlineSize {
				writer.run(func() (err error) {
					return overflow.Recycle(block, leafVal)
				})
			}
			item := writer.extend()
			item.prev.beg = writer.list.tail.page.tail.beg
			item.prev.end = writer.index
			item.defined = true
			item.key = b2s(writer.list.tail.page.LeafKey(writer.index))
			if len(val) > writer.valInlineSize {
				valInlineSize := writer.valInlineSize
				writer.run(func() (err error) {
					var head []byte
					head, err = overflow.Write(block, val, valInlineSize)
					item.val = b2s(head)
					return
				})
			} else {
				item.val = b2s(val)
			}
			writer.list.tail.page.tail.beg = writer.index + 1
		} else {
			// add
			item := writer.extend()
			item.prev.beg = writer.list.tail.page.tail.beg
			item.prev.end = writer.index
			item.defined = true
			if len(key) > writer.keyInlineSize {
				keyInlineSize := writer.keyInlineSize
				writer.run(func() (err error) {
					var head []byte
					head, err = overflow.Write(block, key, keyInlineSize)
					item.key = b2s(head)
					return
				})
			} else {
				item.key = b2s(key)
			}
			if len(val) > writer.valInlineSize {
				valInlineSize := writer.valInlineSize
				writer.run(func() (err error) {
					var head []byte
					head, err = overflow.Write(block, val, valInlineSize)
					item.val = b2s(head)
					return
				})
			} else {
				item.val = b2s(val)
			}
			writer.list.tail.page.tail.beg = writer.index
		}
	} else if writer.found {
		// delete
		if leafKey := writer.list.tail.page.LeafKey(writer.index); len(leafKey) > writer.keyInlineSize {
			writer.run(func() (err error) {
				return overflow.Recycle(block, leafKey)
			})
		}
		if leafVal := writer.list.tail.page.LeafVal(writer.index); len(leafVal) > writer.valInlineSize {
			writer.run(func() (err error) {
				return overflow.Recycle(block, leafVal)
			})
		}
		if writer.index == writer.list.tail.page.tail.beg {
			writer.list.tail.page.tail.beg++
		} else {
			item := writer.extend()
			item.prev.beg = writer.list.tail.page.tail.beg
			item.prev.end = writer.index
			item.defined = false
			writer.list.tail.page.tail.beg = writer.index + 1
		}
	}
}

func (writer *itemWriter[B]) extend() (item *leafItem) {
	if writer.tail == nil {
		item = &writer.list.tail.page.head
	} else {
		item = writer.tail.extend()
	}
	writer.tail = item
	return
}

func writeBranch[B ReadWrite](block B, makePage func(int) []byte, high uint8, root Page, pages pages, leaf *leafNode) (uint8, Page, error) {
	if leaf == nil {
		// if high == 0 {
		// 	return writeRoot(block, makePage, high, root.LeafItems(0, root.Count()))
		// }
		return writeRoot(block, makePage, high, root.BranchItems(0, root.Count()))
	}

	if len(leaf.level) < 2 {
		if len(leaf.level) == 0 {
			return writeRoot(block, makePage, 0, LeafItems(leaf.items))
		}

		page, err := writeRootNodes(block, root, leaf)
		if err != nil {
			return 0, nil, err
		}
		return writeRoot(block, makePage, 1, BranchItems(page.items))
	}

	branch, err := writeBranchNodes(block, pages, leaf)
	if err != nil {
		return 0, nil, err
	}
	if len(branch.level) == 0 {
		return writeRoot(block, makePage, 1, BranchItems(branch.items))
	}

	for high = 1; len(branch.level) > 1; high++ {
		branch, err = writeBranchNodes(block, pages, branch)
		if err != nil {
			return 0, nil, err
		}
		if len(branch.level) == 0 {
			high++
			return writeRoot(block, makePage, high, BranchItems(branch.items))
		}
	}

	page, err := writeRootNodes(block, root, branch)
	if err != nil {
		return 0, nil, err
	}
	high++
	return writeRoot(block, makePage, high, BranchItems(page.items))
}

func writeRootNodes[
	B ReadWrite,
	V BlockID | []byte,
	Items items[V],
	Item branchItem | leafItem,
	ItemPtr itemPtr[V, Items, Item],
](block B, root Page, head *node[V, Items, Item, ItemPtr]) (branch *branchPage, err error) {
	head.prev.beg = 0
	head.prev.end = head.level[0].Index

	prev := head
	node := prev.next
	for node != nil {
		node.prev.beg = prev.prev.end + 1
		node.prev.end = node.level[0].Index
		prev = node
		node = prev.next
	}

	branch = new(branchPage)
	branch.Page = root
	branch.tail.beg = prev.prev.end + 1
	branch.tail.end = prev.level[0].Count

	err = writeNodes(block, branch, head)
	return
}

func writeBranchNodes[
	B ReadWrite,
	V BlockID | []byte,
	Items items[V],
	Item branchItem | leafItem,
	ItemPtr itemPtr[V, Items, Item],
](block B, pages pages, head *node[V, Items, Item, ItemPtr]) (branch *branchNode, err error) {
	high := len(head.level) - 1

	head.prev.beg = 0
	head.prev.end = head.level[high].Index

	writer := writer[B, V, Items, Item, ItemPtr]{block: block, pages: pages}
	var pending list[V, Items, Item, ItemPtr]

	prev := head
	node := head.next
	for node != nil {
		node.prev.end = node.level[high].Index
		if nextTo, samePage := prev.level.compare(node.level); samePage {
			node.prev.beg = prev.prev.end + 1
			if !nextTo {
				if pending.head == nil {
					pending.head = head
				}
				pending.tail = prev
				head = node
			}
		} else {
			node.prev.beg = 0
			if nextTo {
				level := prev.level[:high]
				branch := pending.head
				if branch != nil {
					pending.tail.next = nil
					pending.head = nil
					pending.tail = nil
				}
				tail := head.prev
				blockID := prev.level[high].BlockID
				writer.write(level, branch, tail, blockID)
				head.prev = seg{}
			} else {
				level := prev.level[:high]
				branch := pending.head
				if branch != nil {
					pending.head = nil
					pending.tail = nil
				} else {
					branch = head
				}
				prev.next = nil
				tail := seg{prev.prev.end + 1, prev.level[high].Count}
				blockID := prev.level[high].BlockID
				writer.write(level, branch, tail, blockID)
				head = node
			}
		}
		prev = node
		node = prev.next
	}
	{
		level := prev.level[:high]
		branch := pending.head
		if branch == nil {
			branch = head
		}
		tail := seg{prev.prev.end + 1, prev.level[high].Count}
		blockID := prev.level[high].BlockID
		writer.write(level, branch, tail, blockID)
	}
	if writer.list.trunk() {
		recycleTrunk(block, writer.list.head)
	}

	return writer.list.head, writer.wait()
}

func recycleTrunk[
	B ReadWrite,
	V BlockID | []byte,
	Items items[V],
	Item branchItem | leafItem,
	ItemPtr itemPtr[V, Items, Item],
](block B, node *node[V, Items, Item, ItemPtr]) {
	pageIDs := make(map[BlockID]struct{})
	for node != nil {
		for i := 1; i < len(node.level); i++ {
			pageIDs[node.level[i].BlockID] = struct{}{}
		}
		node.level = nil
		node = node.next
	}
	for blockID := range pageIDs {
		block.RecycleBlock(blockID)
	}
}

// writer should stack-only; no escape
type writer[
	B ReadWrite,
	V BlockID | []byte,
	Items items[V],
	Item branchItem | leafItem,
	ItemPtr itemPtr[V, Items, Item],
] struct {
	block B
	pages
	task
	list[BlockID, BranchItems, branchItem, *branchItem]
}

func (writer *writer[B, V, Items, Item, ItemPtr]) write(
	level Level,
	head *node[V, Items, Item, ItemPtr],
	tail seg, blockID BlockID,
) {
	branch := writer.extend()
	branch.level = level
	branch.page.tail = tail
	branch.Page = writer.pages[blockID]
	if head != nil {
		block := writer.block
		writer.run(func() (err error) {
			err = writeNodes(block, &branch.page, head)
			block.RecycleBlock(blockID)
			return
		})
	} else {
		writer.block.RecycleBlock(blockID)
	}
}

type pages map[BlockID]Page

func recyclePages[B ReadWrite](block B, pages pages) {
	for _, page := range pages {
		block.RecycleBuffer(page)
	}
	clear(pages)
}
