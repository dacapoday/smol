package kv

import (
	"os"

	"github.com/dacapoday/smol/block"
	"github.com/dacapoday/smol/bptree"
	"github.com/dacapoday/smol/btree"
	"github.com/dacapoday/smol/iterator"
)

// Iterator extends iterator.Iterator with Clone and Close.
// Captures an immutable snapshot for consistent reads.
type Iterator[Iter iterator.Iterator] interface {
	iterator.Iterator
	// Clone creates an independent copy at the same position.
	// Shares snapshot but maintains separate position state.
	Clone() Iter
	// Close releases resources held by the iterator.
	Close()
}

// DBIter is an iterator for file-based KV stores.
type DBIter = Iter[*os.File]

var _ Iterator[DBIter] = DBIter{}

// Iter is an iterator over a KV store snapshot.
type Iter[F File] struct {
	ator *iter[F]
}

type iter[F File] = struct {
	ckpt block.HeapCheckpoint
	bptree.Reader[*block.Heap[F]]
}

// Iter creates a new iterator over the key-value store.
// Captures a consistent snapshot at the current moment.
//
// Important: Caller must call Close to release resources.
func (kv *KV[F]) Iter() Iter[F] {
	iter := new(iter[F])
	if root, ckpt := kv.atom.Acquire(); ckpt != nil {
		iter.ckpt = ckpt
		iter.Load(&kv.block, root.page, int(root.klen), int(root.vlen), root.high)
	}
	return Iter[F]{iter}
}

// Clone creates an independent copy at current position.
func (kv Iter[F]) Clone() Iter[F] {
	iter := new(iter[F])
	if kv.ator.ckpt != nil {
		kv.ator.ckpt.Acquire()
		iter.ckpt = kv.ator.ckpt
		iter.LoadFrom(&kv.ator.Reader)
	}
	return Iter[F]{iter}
}

// Close releases resources held by the iterator.
func (iter Iter[F]) Close() {
	if iter.ator.ckpt != nil {
		iter.ator.ckpt.Release()
		iter.ator.ckpt = nil
		iter.ator.Close()
	}
}

// Valid returns true if positioned at a valid item.
func (iter Iter[F]) Valid() bool {
	return iter.ator.Valid()
}

// Error returns any error encountered during iteration.
func (iter Iter[F]) Error() error {
	return iter.ator.Error()
}

// Key returns the current key, or nil if invalid.
//
// Warning: Returned slice is valid only until next method call.
func (iter Iter[F]) Key() []byte {
	return iter.ator.Key()
}

// Val returns the current value, or nil if invalid or deleted.
//
// Warning: Returned slice is valid only until next method call.
func (iter Iter[F]) Val() []byte {
	return iter.ator.Val()
}

// Next advances to the next item.
func (iter Iter[F]) Next() bool {
	return iter.ator.Next()
}

// Prev moves to the previous item.
func (iter Iter[F]) Prev() bool {
	return iter.ator.Prev()
}

// SeekFirst positions at the first key.
func (iter Iter[F]) SeekFirst() bool {
	return iter.ator.SeekFirst()
}

// SeekLast positions at the last key.
func (iter Iter[F]) SeekLast() bool {
	return iter.ator.SeekLast()
}

// Seek positions at the first key >= the given key.
func (iter Iter[F]) Seek(key []byte) bool {
	return iter.ator.Seek(key)
}

// Iter creates a new iterator over the transaction's view.
// Combines pending changes with the base snapshot.
//
// Important: Caller must call Close to release resources.
func (tx *Tx[Iter]) Iter() (iter TxIter[Iter]) {
	iter.ator = new(iterator.Combine[btree.Iter, Iter])
	iter.ator.Load(tx.pending.Iter(), tx.snapshot.Clone(), nil)
	return
}

var _ Iterator[TxIter[DBIter]] = TxIter[DBIter]{}

// TxIter is an iterator over a transaction's view.
// Merges pending changes with the base snapshot.
// Implements iterator.Iterator interface.
type TxIter[Iter Iterator[Iter]] struct {
	ator *iterator.Combine[btree.Iter, Iter]
}

// Clone creates an independent copy at current position.
func (iter TxIter[Iter]) Clone() (newIter TxIter[Iter]) {
	newIter.ator = new(iterator.Combine[btree.Iter, Iter])
	newIter.ator.Load(iter.ator.Over().Clone(), iter.ator.Base().Clone(), iter.ator)
	return
}

// Close releases resources held by the iterator.
func (iter TxIter[Iter]) Close() {
	iter.ator.Base().Close()
	iter.ator.Load(iter.ator.Over(), iter.ator.Base(), nil)
}

// Valid returns true if positioned at a valid item.
func (iter TxIter[Iter]) Valid() bool {
	return iter.ator.Valid()
}

// Error returns any error encountered during iteration.
func (iter TxIter[Iter]) Error() error {
	return iter.ator.Error()
}

// Key returns the current key, or nil if invalid.
//
// Warning: Returned slice is valid only until next method call.
func (iter TxIter[Iter]) Key() []byte {
	return iter.ator.Key()
}

// Val returns the current value, or nil if invalid or deleted.
//
// Warning: Returned slice is valid only until next method call.
func (iter TxIter[Iter]) Val() []byte {
	return iter.ator.Val()
}

// Next advances to the next item.
func (iter TxIter[Iter]) Next() bool {
	return iter.ator.Next()
}

// Prev moves to the previous item.
func (iter TxIter[Iter]) Prev() bool {
	return iter.ator.Prev()
}

// SeekFirst positions at the first key.
func (iter TxIter[Iter]) SeekFirst() bool {
	return iter.ator.SeekFirst()
}

// SeekLast positions at the last key.
func (iter TxIter[Iter]) SeekLast() bool {
	return iter.ator.SeekLast()
}

// Seek positions at the first key >= the given key.
func (iter TxIter[Iter]) Seek(key []byte) bool {
	return iter.ator.Seek(key)
}
