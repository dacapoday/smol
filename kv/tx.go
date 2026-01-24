package kv

import (
	"bytes"

	"github.com/dacapoday/smol/btree"
)

// Begin starts a new transaction with Read Committed isolation.
// Captures a snapshot and accumulates changes in memory.
//
// Writes are serialized. Uncommitted changes are isolated until Commit.
func (kv *KV[F]) Begin() (tx *Tx[Iter[F]]) {
	tx = new(Tx[Iter[F]])
	tx.Begin(kv.Iter(), kv.commitSortedChanges)
	return
}

// Tx represents a transaction with Read Committed isolation.
// Buffers changes in memory until Commit.
type Tx[Iter Iterator[Iter]] struct {
	commit   Commit
	snapshot Iter
	pending  btree.BTree
}

// Commit is a function type for committing sorted changes.
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
// Transaction cannot be reused after rollback.
func (tx *Tx[Iter]) Rollback() {
	if tx.commit == nil {
		return
	}
	tx.close()
}

// Commit atomically writes all pending changes to the store.
// Returns immediately if no changes were made.
// Transaction is closed after commit (successful or not).
func (tx *Tx[Iter]) Commit() (err error) {
	if tx.commit == nil {
		err = ErrClosed
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
// Checks pending changes first, then the snapshot.
// Returns nil if key does not exist.
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
// Changes are only visible within this transaction until Commit.
// Pass nil value to delete a key.
//
// Warning: Caller must not modify key or val after calling Set.
func (tx *Tx[Iter]) Set(key, val []byte) {
	tx.pending.Set(key, val)
}
