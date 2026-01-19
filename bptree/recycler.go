// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package bptree

import (
	"github.com/dacapoday/smol/overflow"
)

// Recycle releases all blocks used by the B+ tree.
func Recycle[B ReadWrite](block B, root Page, keyInlineSize, valInlineSize int) (err error) {
	task := new(task)
	recycle(block, task, root, keyInlineSize, valInlineSize)
	return task.wait()
}

func recycle[B ReadWrite](block B, task *task, page Page, keyInlineSize, valInlineSize int) {
	count := page.Count()
	if page.IsLeaf() {
		for i := range count {
			if key := page.LeafKey(i); len(key) > keyInlineSize {
				overflowID := overflowID(key)
				task.run(func() error {
					return overflow.Recycle(block, overflowID)
				})
			}
			if val := page.LeafVal(i); len(val) > valInlineSize {
				overflowID := overflowID(val)
				task.run(func() error {
					return overflow.Recycle(block, overflowID)
				})
			}
		}
	} else {
		for i := range count {
			blockID := page.BranchID(i)
			task.run(func() error {
				return recycleBlock(block, task, blockID, keyInlineSize, valInlineSize)
			})
			block.RecycleBlock(blockID)
		}
	}
}

func recycleBlock[B ReadWrite](block B, task *task, blockID BlockID, keyInlineSize, valInlineSize int) (err error) {
	buffer := block.AllocateBuffer()
	err = block.ReadBlock(blockID, buffer, func(buffer []byte) {
		recycle(block, task, Page(buffer), keyInlineSize, valInlineSize)
	})
	block.RecycleBuffer(buffer)
	return
}
