// Package kv implements a disk-based key-value store built on
// a copy-on-write B+ tree. Keys are maintained in lexicographic order with
// CRC32-protected block storage for data integrity.
//
// Concurrency and isolation:
//   - MVCC snapshot isolation
//   - Read Committed transaction isolation level
//   - Concurrent reads and writes supported
//
// Capacity limits:
//   - File size: minimum 32 KiB, theoretical maximum 64 TiB
//   - Key/value sizes have no hard limit
//   - Recommended: keys < 3258 bytes, values < 13092 bytes
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
	"os"

	"github.com/dacapoday/smol/block"
	"github.com/dacapoday/smol/bptree"
	"github.com/dacapoday/smol/btree"
)

// DB is a specialized KV instance using os.File as the underlying storage.
// It provides a convenient type for working with file-based key-value stores.
type DB = KV[*os.File]

// Open creates or opens a key-value database file at the specified path.
// The file is created with permissions 0600 if it doesn't exist.
// Returns an initialized DB ready for operations, or an error if the file
// cannot be opened or the existing data is corrupted.
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

// KV is a generic key-value store parameterized by file type.
// It wraps a B+ tree with CRC32-protected block storage, providing persistent
// ordered key-value storage with transaction support.
//
// Type parameter F must implement the File interface (typically *os.File or *mem.File).
// Use DB for the common case of file-based storage.
type KV[F File] struct {
	bptree bptree.BPTree[*block.CRC32Heap[F], block.HeapCheckpoint]
	block  block.CRC32Heap[F]
}

// File returns the underlying file handle used by this KV instance.
func (kv *KV[F]) File() F {
	return kv.block.File()
}

// Load initializes the KV store from an existing file.
// Reads and validates the block structure, recovering the B+ tree state
// from the latest checkpoint. Returns an error if the file is corrupted
// or uses an incompatible format.
func (kv *KV[F]) Load(file F) (err error) {
	entry, ckpt, err := kv.block.Load(file, BlockOption{})
	if err != nil {
		return
	}

	err = kv.bptree.Load(&kv.block, entry, ckpt)
	return
}

// BlockOption defines the block storage specification for KV.
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

// Close releases all resources associated with the KV store.
// The underlying file is also closed.
func (kv *KV[F]) Close() (err error) {
	return kv.bptree.Close()
}

// Get retrieves the value for the given key.
// Returns nil if the key does not exist. The returned value is an
// independent copy and safe to modify.
func (kv *KV[F]) Get(key []byte) (val []byte, err error) {
	return kv.bptree.Get(key)
}

// Set inserts or updates a key-value pair.
// To delete a key, pass nil as the value.
func (kv *KV[F]) Set(key []byte, val []byte) (err error) {
	return kv.bptree.Set(key, val)
}

// Batch atomically commits multiple key-value changes.
//
// The changes parameter is an iterator that yields key-value pairs.
// Keys do not need to be pre-sorted. A nil value indicates deletion.
// All yielded keys and values must remain valid until Batch returns,
// not just during iteration.
func (kv *KV[F]) Batch(changes func(yield func([]byte, []byte) bool)) error {
	var batch btree.BTree
	for k, v := range changes {
		batch.Set(k, v)
	}
	return kv.bptree.CommitSortedChanges(batch.Items, 4096)
}
