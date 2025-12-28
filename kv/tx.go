package kv

import (
	"bytes"

	"github.com/dacapoday/smol/bptree"
	"github.com/dacapoday/smol/btree"
)

// Begin starts a new transaction with Read Committed isolation level.
// The transaction captures a snapshot of the current state and allows
// accumulating changes in memory before atomically committing them.
//
// Multiple transactions can read concurrently, but writes are serialized.
// Uncommitted changes are isolated from other readers until Commit.
func (kv *KV[F]) Begin() (tx *Tx[Iter[F]]) {
	tx = new(Tx[Iter[F]])
	tx.Begin(kv.Iter(), kv.Batch)
	return
}

// Tx represents a transaction with Read Committed isolation.
// Changes are buffered in memory until Commit, allowing bulk updates
// without affecting concurrent readers. The transaction maintains a
// snapshot view and merges pending changes on read.
type Tx[Iter Iterator[Iter]] struct {
	commit   Commit
	snapshot Iter
	pending  btree.BTree
}

// Commit is a function type for committing sorted changes to the store.
type Commit = func(sortedChanges func(yield func([]byte, []byte) bool)) error

// Begin initializes the transaction with a snapshot and commit function.
func (tx *Tx[Iter]) Begin(snapshot Iter, commit Commit) {
	if tx.commit != nil {
		tx.close()
	}
	tx.commit = commit
	tx.snapshot = snapshot
}

func (tx *Tx[Iter]) close() {
	tx.commit = nil
	tx.snapshot.Close()
	var nilSnapshot Iter
	tx.snapshot = nilSnapshot
	tx.pending.Reset()
}

// Rollback discards all pending changes and closes the transaction.
// After rollback, the transaction cannot be reused and any operations
// will return ErrClosed.
func (tx *Tx[Iter]) Rollback() {
	if tx.commit == nil {
		return
	}
	tx.close()
}

// Commit atomically writes all pending changes to the store.
// If no changes were made, returns immediately without error.
// After commit (successful or not), the transaction is closed.
func (tx *Tx[Iter]) Commit() (err error) {
	if tx.commit == nil {
		err = bptree.ErrClosed
		return
	}
	if tx.pending.Empty() {
		return
	}
	err = tx.commit(tx.pending.Items)
	tx.close()
	return
}

// Get retrieves a value within the transaction's view.
// Checks pending changes first, then falls back to the snapshot.
// Returns nil if the key does not exist in either location.
func (tx *Tx[Iter]) Get(key []byte) (val []byte, err error) {
	val, found := tx.pending.Get(key)
	if found {
		return
	}

	if !tx.snapshot.Seek(key) {
		err = tx.snapshot.Error()
		return
	}

	if !bytes.Equal(tx.snapshot.Key(), key) {
		return
	}

	if val = append(val, tx.snapshot.Val()...); val == nil {
		val = []byte{}
	}
	return
}

// Set writes a key-value pair to the transaction's pending changes.
// The change is buffered in memory and will only be visible to other
// operations within this transaction until Commit is called.
// To delete a key, pass nil as the value.
func (tx *Tx[Iter]) Set(key, val []byte) {
	tx.pending.Set(key, val)
}
