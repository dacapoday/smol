package bptree

import (
	"errors"
	"math/rand/v2"
)

// High calculates the height of a B+ tree by traversing from the root page to a leaf.
// Returns 0 for a root-only tree (single leaf page).
func High[B ReadOnly](block B, root Page) (high uint8, err error) {
	var buffer []byte
	var blockID BlockID
	var count uint16
	for {
		count = root.Count()
		if count == 0 {
			err = errors.New("page count is 0")
			return
		}

		if root.IsLeaf() {
			return
		}

		high++

		blockID = root.BranchID(rand.N(count))
		if blockID < 2 {
			err = errors.New("blockID < 2")
			return
		}

		if buffer == nil {
			buffer = block.AllocateBuffer()
			defer block.RecycleBuffer(buffer)
		}
		err = block.ReadBlock(blockID, buffer, nil)
		if err != nil {
			return
		}

		root = Page(buffer)
	}
}
