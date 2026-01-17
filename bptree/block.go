// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package bptree

import "github.com/dacapoday/smol"

type BlockID = smol.BlockID
type Block[C Checkpoint] = smol.Block[C]
type Checkpoint = interface {
	comparable
	smol.Checkpoint
}
type ReadWrite = smol.ReadWrite
type ReadOnly = smol.ReadOnly

// RootBlock represents B+ tree metadata.
type RootBlock interface {
	// High returns tree height, starting from 0 (root-only tree).
	High() uint8

	// Page returns the root page.
	Page() Page

	// KeyInlineSize returns maximum inline key size in a page.
	KeyInlineSize() int

	// ValInlineSize returns maximum inline value size in a page.
	ValInlineSize() int
}

type MakePage interface {
	MakePage(size int) []byte
}
