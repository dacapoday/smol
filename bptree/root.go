// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package bptree

import "iter"

func writeRoot[B ReadWrite, V BlockID | []byte, Items items[V]](block B, h uint8, items Items) (high uint8, root Page, err error) {
	root, branch, err := writeRootPage(block, items)
	if err != nil {
		return
	}
	if root == nil && branch == nil {
		return
	}
	for high = h; branch != nil; high++ {
		root, branch, err = writeRootPage(block, branch)
		if err != nil {
			return
		}
	}
	return
}

func writeRootPage[B ReadWrite, V BlockID | []byte, Items items[V]](block B, items Items) (root Page, branch BranchItems, err error) {
	next, stop := layout(items, block.PageSize())
	defer stop()
	size, items, last := next()
	if size <= HeadSize {
		return
	}
	if last {
		root = make([]byte, size)
		items.encode(root)
		return
	}

	buffer := block.AllocateBuffer()
	defer block.RecycleBuffer(buffer)

	key := items.encode(buffer[:size])

	blockID := block.AllocateBlock()
	if blockID < 2 {
		err = errAllocateFailed(block)
		return
	}

	err = block.WriteBlock(blockID, buffer)
	if err != nil {
		return
	}

	item := new(branchItem)
	branch = item.items
	item.key = b2s(key)
	item.id = blockID
	for {
		size, items, last = next()
		key = items.encode(buffer[:size])

		blockID = block.AllocateBlock()
		if blockID < 2 {
			err = errAllocateFailed(block)
			return
		}

		err = block.WriteBlock(blockID, buffer)
		if err != nil {
			return
		}

		item = item.extend()
		item.key = b2s(key)
		item.id = blockID

		if last {
			return
		}
	}
}

func layout[V BlockID | []byte, Items items[V]](items Items, pageSize int) (func() (int, Items, bool), func()) {
	next, stopPager := pager(items.ItemSize, pageSize)
	chunk, stopGroup := group(items)
	return func() (int, Items, bool) {
		size, count, last := next()
		return size, chunk(count), last
	}, func() { stopPager(); stopGroup() }
}

func pager(items func(yield func(int) bool), pageSize int) (func() (int, uint16, bool), func()) {
	next, stop := iter.Pull(items)
	var itemSize int
	return func() (size int, count uint16, last bool) {
		if itemSize != 0 {
			count++
		}
		size = HeadSize + itemSize
		var ok bool
		for {
			itemSize, ok = next()
			if !ok {
				last = true
				return
			}
			if size+itemSize > pageSize {
				return
			}
			size += itemSize
			count++
		}
	}, stop
}

func group[V BlockID | []byte, Items items[V]](items Items) (func(uint16) Items, func()) {
	next, stop := iter.Pull2(iter.Seq2[[]byte, V](items))
	return func(count uint16) Items {
		return func(yield func([]byte, V) bool) {
			for ; count > 0; count-- {
				k, v, ok := next()
				if !ok {
					return
				}
				if !yield(k, v) {
					for ; count > 0; count-- {
						next()
					}
					return
				}
			}
		}
	}, stop
}
