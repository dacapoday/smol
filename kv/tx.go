package kv

import (
	"bytes"

	"github.com/dacapoday/smol/bptree"
	"github.com/dacapoday/smol/btree"
)

func (kv *KV[F]) Begin() (tx *Tx[Iter[F]]) {
	tx = new(Tx[Iter[F]])
	tx.Begin(kv.Iter(), kv.Batch)
	return
}

type Tx[Iter Iterator[Iter]] struct {
	commit   Commit
	snapshot Iter
	pending  btree.BTree
}

type Commit = func(sortedChanges func(yield func([]byte, []byte) bool)) error

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

func (tx *Tx[Iter]) Rollback() {
	if tx.commit == nil {
		return
	}
	tx.close()
}

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

	val = append(val, tx.snapshot.Val()...)
	if val == nil {
		val = []byte{}
	}
	return
}

func (tx *Tx[Iter]) Set(key, val []byte) {
	tx.pending.Set(key, val)
}
