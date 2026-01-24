// Package kv implements a disk-based key-value store built on
// copy-on-write B+ tree. Keys are maintained in lexicographic order with
// CRC32-protected block storage for data integrity.
//
// Specifications:
//   - File size: minimum 32 KiB, theoretical maximum 64 TiB
//   - Key/value sizes: no hard limit (recommended: keys < 3258 bytes, values < 13092 bytes)
//
// Concurrency:
//   - Thread-safe: concurrent reads and writes supported
//   - Isolation: MVCC snapshot isolation with Read Committed transaction level
//
// Important: Complete transactions (Commit/Rollback) and close iterators (Close)
// promptly to prevent unexpected database file growth due to retained snapshots.
//
// Usage:
//
//	db, err := kv.Open("data.kv")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
//	// Single operations
//	db.Set([]byte("key"), []byte("value"))
//	val, _ := db.Get([]byte("key"))
//
//	// Batch writes
//	db.Batch(func(yield func([]byte, []byte) bool) {
//	    yield([]byte("k1"), []byte("v1"))
//	    yield([]byte("k2"), []byte("v2"))
//	})
//
//	// Transactions
//	tx := db.Begin()
//	tx.Set([]byte("key"), []byte("new"))
//	tx.Commit()
//
//	// Iteration
//	iter := db.Iter()
//	defer iter.Close()
//	for iter.SeekFirst(); iter.Valid(); iter.Next() {
//	    key, val := iter.Key(), iter.Val()
//	    // process key, val
//	}
package kv

import (
	"fmt"
	"math"
	"os"

	"github.com/dacapoday/smol/atom"
	"github.com/dacapoday/smol/block"
	"github.com/dacapoday/smol/bptree"
	"github.com/dacapoday/smol/btree"
)

// DB is a KV instance using os.File as underlying storage.
type DB = KV[*os.File]

// Open creates or opens a database file at the specified path.
// Creates the file with 0600 permissions if it doesn't exist.
// Returns error if the file cannot be opened or contains corrupted data.
func Open(path string) (db *DB, err error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return
	}

	db = new(DB)
	if err = db.Load(file); err != nil {
		db = nil
	}
	return
}

type File = block.File

// KV is a generic key-value store parameterized by file type F.
//
// Type parameter F must implement File interface (*os.File or *mem.File).
// Use DB for file-based storage.
type KV[F File] struct {
	atom atom.Embed[block.Heap[F], *block.Heap[F], block.HeapCheckpoint, kvroot]
}

type kvroot = struct {
	page bptree.Page
	klen uint16
	vlen uint16
	high uint8
}

// File returns the underlying file handle.
func (kv *KV[F]) File() F {
	return kv.atom.Block().File()
}

// Load initializes the KV store from an existing file.
// Recovers B+ tree state from the latest checkpoint.
// Returns error if the file is corrupted or incompatible.
func (kv *KV[F]) Load(file F) (err error) {
	block := kv.atom.Block()
	entry, ckpt, err := block.Load(file, BlockOption{})
	if err != nil {
		return
	}

	var root kvroot
	if entrySize := len(entry); entrySize != 0 {
		page := bptree.Page(entry)
		if page.Count() == 0 {
			err = fmt.Errorf("kv.Load: %w kv entry", ErrUnsupported)
			return
		}
		high, e := bptree.High(block, page)
		if e != nil {
			err = fmt.Errorf("kv.Load: %w", e)
			return
		}
		root.high = high
		root.page = page
	}
	{
		pageSize := block.PageSize()
		maxOverflowSize := math.MaxUint32 * pageSize
		klen, vlen := bptree.InlineSize(pageSize, 5, maxOverflowSize, maxOverflowSize)
		root.klen = uint16(klen)
		root.vlen = uint16(vlen)
	}

	kv.atom.Load(ckpt, root)
	return
}

// BlockOption defines block storage specification for KV.
// Magic code: "DICT", block size: 16KB, checkpoint retention: 0.
//
// Advanced features can be accessed through the bptree and block packages
// using this option with KV.File().
type BlockOption struct{}

func (o BlockOption) MagicCode() [4]byte {
	return [4]byte{'D', 'I', 'C', 'T'}
}

func (o BlockOption) ReadOnly() bool {
	return false
}

func (o BlockOption) IgnoreInvalidFreelist() bool {
	return false
}

func (o BlockOption) RetainCheckpoints() uint8 {
	return 0
}

func (o BlockOption) BlockSize() int {
	return 1 << 14
}

// Close releases all resources and closes the underlying file.
func (kv *KV[F]) Close() (err error) {
	return kv.atom.Close()
}

// Get retrieves the value for the given key.
// Returns nil if key does not exist.
// Returned value is safe to modify.
func (kv *KV[F]) Get(key []byte) (val []byte, err error) {
	root, ckpt := kv.atom.Acquire()
	if ckpt == nil {
		err = ErrClosed
		return
	}
	val, err = bptree.Get(kv.atom.Block(), root.page, int(root.klen), int(root.vlen), root.high, nil, key)
	ckpt.Release()
	return
}

// Set inserts or updates a key-value pair.
// Pass nil value to delete a key.
func (kv *KV[F]) Set(key []byte, val []byte) (err error) {
	return kv.commitSortedChanges(func(yield func([]byte, []byte) bool) { yield(key, val) })
}

// Batch atomically commits multiple key-value changes.
// Keys do not need to be sorted. Pass nil value to delete a key.
//
// Warning: Caller must not modify yielded keys/values until Batch returns.
func (kv *KV[F]) Batch(changes func(yield func([]byte, []byte) bool)) error {
	var batch btree.BTree
	for k, v := range changes {
		batch.Set(k, v)
	}
	return kv.commitSortedChanges(batch.Items)
}

func (kv *KV[F]) commitSortedChanges(sortedChanges func(func([]byte, []byte) bool)) error {
	return kv.atom.Swap(func(block *block.Heap[F], root kvroot) (entry []byte, newRoot kvroot, err error) {
		high, page, err := bptree.WriteSortedChanges(block,
			root.page, int(root.klen), int(root.vlen), root.high,
			sortedChanges)
		if err != nil {
			return
		}

		entry = page
		newRoot = kvroot{high: high, page: page, klen: root.klen, vlen: root.vlen}
		return
	})
}
