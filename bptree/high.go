// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package bptree

// High calculates the height of a B+ tree by traversing from the root page to a leaf.
// Returns 0 for a root-only tree (single leaf page).
func High[B ReadOnly](block B, root Page) (high uint8, err error) {
	var buffer []byte
	for !root.IsLeaf() {
		high++
		if buffer == nil {
			buffer = block.AllocateBuffer()
			defer block.RecycleBuffer(buffer)
		}
		if err = block.ReadBlock(root.BranchID(0), buffer, nil); err != nil {
			return
		}
		root = Page(buffer)
	}
	return
}
