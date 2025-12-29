// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

// Package bptree implements a copy-on-write B+ tree with MVCC snapshot isolation.
package bptree

import (
	"fmt"
	"math"
	"sync"
)

// BPTree is the default container implementation for the copy-on-write B+ tree.
//
// Keys and values can be arbitrarily large.
// For best performance, keep them within inline size limits from
// root.KeyInlineSize() and root.ValInlineSize(). Larger entries use overflow storage.
type BPTree[B Block[C], C Checkpoint] struct {
	block B
	root  *Root[C]
	view  sync.RWMutex
	mutex sync.Mutex
}

func (bptree *BPTree[B, C]) Block() B {
	return bptree.block
}

func (bptree *BPTree[B, C]) Load(block B, entry []byte, ckpt C) (err error) {
	bptree.mutex.Lock()
	defer bptree.mutex.Unlock()
	bptree.view.Lock()
	defer bptree.view.Unlock()

	root, err := loadRoot(block, entry, ckpt)
	if err != nil {
		return
	}

	bptree.root = root
	bptree.block = block
	return
}

func (bptree *BPTree[B, C]) Close() (err error) {
	bptree.mutex.Lock()
	defer bptree.mutex.Unlock()
	bptree.view.Lock()
	defer bptree.view.Unlock()

	if bptree.root == nil {
		return
	}

	bptree.root.ckpt.Release()
	bptree.root = nil

	err = bptree.block.Close()
	var nilBlock B
	bptree.block = nilBlock
	return
}

// Get retrieves the value for the given key.
// Returns nil if key does not exist.
// Returned value is safe to modify.
func (bptree *BPTree[B, C]) Get(key []byte) (val []byte, err error) {
	root := bptree.AcquireRoot()
	if root == nil {
		err = ErrClosed
		return
	}
	defer root.Checkpoint().Release()

	return Get(bptree.block, root, nil, key)
}

// Set inserts or updates a key-value pair.
// Pass nil value to delete a key.
func (bptree *BPTree[B, C]) Set(key []byte, val []byte) (err error) {
	return bptree.CommitSortedChanges(func(yield func([]byte, []byte) bool) { yield(key, val) }, math.MaxUint32)
}

// CommitSortedChanges atomically writes a batch of sorted changes, creating a new
// tree version without modifying the original.
//
// sortedChanges must yield key-value pairs in ascending lexicographic order.
// A nil value indicates deletion of the key. All yielded keys and values must
// remain valid until the method returns, not just during iteration.
//
// maxLoadedPages limits pages loaded at once to control memory use.
func (bptree *BPTree[B, C]) CommitSortedChanges(sortedChanges func(func([]byte, []byte) bool), maxLoadedPages uint32) (err error) {
	bptree.mutex.Lock()
	defer bptree.mutex.Unlock()

	oldRoot := bptree.root
	if oldRoot == nil {
		return ErrClosed
	}

	high, page, err := WriteSortedChanges(bptree.block, oldRoot, sortedChanges, maxLoadedPages)
	if err != nil {
		bptree.block.Rollback()
		return
	}

	ckpt, err := bptree.block.Commit(page)
	if err != nil {
		return
	}

	newRoot := &Root[C]{
		ckpt: ckpt,
		high: high,
		page: page,
		klen: oldRoot.klen,
		vlen: oldRoot.vlen,
	}

	bptree.view.Lock()
	bptree.root = newRoot
	oldRoot.ckpt.Release()
	bptree.view.Unlock()
	return
}

// AcquireRoot returns a snapshot of the current root with an acquired reference.
// The caller must call Release on the checkpoint when done.
func (bptree *BPTree[B, C]) AcquireRoot() (root *Root[C]) {
	bptree.view.RLock()
	if root = bptree.root; root != nil {
		root.ckpt.Acquire()
	}
	bptree.view.RUnlock()
	return
}

// Root represents a snapshot of the B+ tree root metadata.
// It contains the checkpoint, root page, and inline size configuration.
type Root[C Checkpoint] struct {
	ckpt C
	page Page
	klen uint16
	vlen uint16
	high uint8
}

func loadRoot[B Block[C], C Checkpoint](block B, entry []byte, ckpt C) (*Root[C], error) {
	root := new(Root[C])
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
	root.ckpt = ckpt

	return root, nil
}

// Checkpoint returns the checkpoint associated with this root snapshot.
func (root *Root[C]) Checkpoint() C {
	return root.ckpt
}

var _ RootBlock = (*Root[Checkpoint])(nil)

// High returns the tree height (0 for root-only tree).
func (root *Root[C]) High() uint8 {
	return root.high
}

// Page returns the root page of the tree.
func (root *Root[C]) Page() Page {
	return root.page
}

// KeyInlineSize returns the maximum inline key size stored in pages.
func (root *Root[C]) KeyInlineSize() int {
	return int(root.klen)
}

// ValInlineSize returns the maximum inline value size stored in pages.
func (root *Root[C]) ValInlineSize() int {
	return int(root.vlen)
}
