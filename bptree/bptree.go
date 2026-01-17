// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

// Package bptree implements a copy-on-write B+ tree with MVCC snapshot isolation.
package bptree

import (
	"fmt"
	"math"

	"github.com/dacapoday/smol/internal/atom"
)

// BPTree is the default container implementation for the copy-on-write B+ tree.
//
// Keys and values can be arbitrarily large.
// For best performance, keep them within inline size limits from
// root.KeyInlineSize() and root.ValInlineSize(). Larger entries use overflow storage.
type BPTree[B Block[C], C Checkpoint] struct {
	atom atom.Ref[B, C, *Root]
}

func (bptree *BPTree[B, C]) Block() B {
	return bptree.atom.Block()
}

func (bptree *BPTree[B, C]) Load(block B, ckpt C, root *Root) {
	bptree.atom.Load(block, ckpt, root)
}

func (bptree *BPTree[B, C]) Close() (err error) {
	return bptree.atom.Close()
}

// Get retrieves the value for the given key.
// Returns nil if key does not exist.
// Returned value is safe to modify.
func (bptree *BPTree[B, C]) Get(key []byte) (val []byte, err error) {
	ckpt, root := bptree.atom.Acquire()
	var nilCkpt C
	if ckpt == nilCkpt {
		err = ErrClosed
		return
	}

	val, err = Get(bptree.atom.Block(), root, nil, key)
	ckpt.Release()
	return
}

// Set inserts or updates a key-value pair.
// Pass nil value to delete a key.
func (bptree *BPTree[B, C]) Set(key []byte, val []byte) (err error) {
	return bptree.CommitSortedChanges(func(yield func([]byte, []byte) bool) { yield(key, val) })
}

// CommitSortedChanges atomically writes a batch of sorted changes, creating a new
// tree version without modifying the original.
//
// sortedChanges must yield key-value pairs in ascending lexicographic order.
// A nil value indicates deletion of the key. All yielded keys and values must
// remain valid until the method returns, not just during iteration.
//
// maxLoadedPages limits pages loaded at once to control memory use.
func (bptree *BPTree[B, C]) CommitSortedChanges(sortedChanges func(func([]byte, []byte) bool)) (err error) {
	return bptree.atom.Swap(func(block B, root *Root) (entry []byte, newRoot *Root, err error) {
		high, page, err := WriteSortedChanges(block, root, sortedChanges)
		if err != nil {
			return
		}

		entry = page
		newRoot = &Root{
			high: high,
			page: page,
			klen: root.klen,
			vlen: root.vlen,
		}
		return
	})
}

// AcquireRoot returns a snapshot of the current root with an acquired reference.
// The caller must call Release on the checkpoint when done.
func (bptree *BPTree[B, C]) AcquireRoot() (root *Root, ckpt C) {
	ckpt, root = bptree.atom.Acquire()
	return
}

// Root represents a snapshot of the B+ tree root metadata.
// It contains the checkpoint, root page, and inline size configuration.
type Root struct {
	page Page
	klen uint16
	vlen uint16
	high uint8
}

func LoadRoot[B ReadOnly](block B, entry []byte) (*Root, error) {
	root := new(Root)
	if entrySize := len(entry); entrySize != 0 {
		page := Page(entry)
		if page.Count() == 0 {
			return nil, fmt.Errorf("entry is %w", ErrUnsupported)
		}

		high, err := High(block, page)
		if err != nil {
			return nil, fmt.Errorf("High: %w", err)
		}

		root.high = high
		root.page = page
	}
	{
		pageSize := block.PageSize()
		maxOverflowSize := math.MaxUint32 * pageSize // maxOverflowSize := (math.MaxUint32 - 2) * (pageSize - HeadSize - 4)
		klen, vlen := InlineSize(pageSize, 5, maxOverflowSize, maxOverflowSize)

		root.klen = uint16(klen)
		root.vlen = uint16(vlen)
	}

	return root, nil
}

// High returns the tree height (0 for root-only tree).
func (root *Root) High() uint8 {
	return root.high
}

// Page returns the root page of the tree.
func (root *Root) Page() Page {
	return root.page
}

// KeyInlineSize returns the maximum inline key size stored in pages.
func (root *Root) KeyInlineSize() int {
	return int(root.klen)
}

// ValInlineSize returns the maximum inline value size stored in pages.
func (root *Root) ValInlineSize() int {
	return int(root.vlen)
}
