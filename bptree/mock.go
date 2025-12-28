// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package bptree

import (
	"fmt"
)

// newMockLeafItems creates LeafItems with lexicographically sorted keys
func newMockLeafItems(count int, keyLen int, valLen int) LeafItems {
	return LeafItems(func(yield func([]byte, []byte) bool) {
		for i := range count {
			// Generate lexicographically sorted keys using zero-padded format
			key := fmt.Sprintf("%0*d", keyLen, i)

			// Generate deterministic values based on index
			val := make([]byte, valLen)
			for j := range val {
				val[j] = byte((i + j) % 256)
			}

			if !yield([]byte(key), val) {
				return
			}
		}
	})
}

// newMockBranchItems creates BranchItems with lexicographically sorted keys
// PageIDs start from startID and are guaranteed to be >= 2
func newMockBranchItems(count int, keyLen int, startID BlockID) BranchItems {
	// Ensure startID is at least 2
	if startID < 2 {
		startID = 2
	}

	return BranchItems(func(yield func([]byte, BlockID) bool) {
		for i := range count {
			// Generate lexicographically sorted keys using zero-padded format
			key := fmt.Sprintf("%0*d", keyLen, i)
			blockID := startID + BlockID(i)

			if !yield([]byte(key), blockID) {
				return
			}
		}
	})
}

type option struct{}

func (o option) MagicCode() [4]byte          { return [4]byte{'t', 'r', 'e', 'e'} }
func (o option) ReadOnly() bool              { return false }
func (o option) IgnoreInvalidFreelist() bool { return false }
func (o option) RetainCheckpoints() uint8    { return 0 }
func (o option) BlockSize() int              { return 512 }
