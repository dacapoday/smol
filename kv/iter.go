package kv

import (
	"os"

	"github.com/dacapoday/smol/block"
	"github.com/dacapoday/smol/bptree"
	"github.com/dacapoday/smol/btree"
	"github.com/dacapoday/smol/iterator"
)

// Iterator extends the standard iterator.Iterator interface with Clone and Close.
// Iterators represent an immutable snapshot of the key-value store at the time
// of creation, providing consistent reads even as the store is modified.
type Iterator[Iter iterator.Iterator] interface {
	iterator.Iterator
	// Clone creates an independent copy of this iterator at the same position.
	// The clone shares the same snapshot but maintains separate position state.
	Clone() Iter
	// Close releases resources held by this iterator.
	// The snapshot reference is released, allowing garbage collection.
	Close()
}

// DBIter is a specialized iterator for file-based KV stores.
type DBIter = Iter[*os.File]

var _ Iterator[DBIter] = DBIter{}

// Iter creates a new iterator over the key-value store.
// The iterator captures a consistent snapshot at the current moment.
// The caller must call Close when done to release resources.
func (kv *KV[F]) Iter() (iter Iter[F]) {
	iter.ator = kv.bptree.Iterator()
	return
}

// Iter is an iterator over a KV store snapshot.
// It wraps the underlying B+ tree iterator, providing navigation
// over sorted key-value pairs.
type Iter[F File] struct {
	ator bptree.Iterator[*block.CRC32Heap[F], block.HeapCheckpoint]
}

func (iter Iter[F]) Clone() (newIter Iter[F]) {
	newIter.ator = iter.ator.Clone()
	return
}

func (iter Iter[F]) Close() {
	iter.ator.Close()
}

func (iter Iter[F]) Valid() bool {
	return iter.ator.Valid()
}

func (iter Iter[F]) Error() error {
	return iter.ator.Error()
}

func (iter Iter[F]) Key() []byte {
	return iter.ator.Key()
}

func (iter Iter[F]) Val() []byte {
	return iter.ator.Val()
}

func (iter Iter[F]) Next() bool {
	return iter.ator.Next()
}

func (iter Iter[F]) Prev() bool {
	return iter.ator.Prev()
}

func (iter Iter[F]) SeekFirst() bool {
	return iter.ator.SeekFirst()
}

func (iter Iter[F]) SeekLast() bool {
	return iter.ator.SeekLast()
}

func (iter Iter[F]) Seek(key []byte) bool {
	return iter.ator.Seek(key)
}

// Iter creates a new iterator over the transaction's view of the data.
// This combines pending uncommitted changes with the base snapshot,
// providing a consistent view of what will be committed.
func (tx *Tx[Iter]) Iter() (iter TxIter[Iter]) {
	iter.ator = new(iterator.Combine[btree.Iter, Iter])
	iter.ator.Load(tx.pending.Iter(), tx.snapshot.Clone(), nil)
	return
}

var _ Iterator[TxIter[DBIter]] = TxIter[DBIter]{}

// TxIter is an iterator over a transaction's view of the data.
// It merges the transaction's pending changes with the base snapshot,
// showing the combined result as it would appear after commit.
type TxIter[Iter Iterator[Iter]] struct {
	ator *iterator.Combine[btree.Iter, Iter]
}

func (iter TxIter[Iter]) Clone() (newIter TxIter[Iter]) {
	newIter.ator = new(iterator.Combine[btree.Iter, Iter])
	newIter.ator.Load(iter.ator.Over().Clone(), iter.ator.Base().Clone(), iter.ator)
	return
}

func (iter TxIter[Iter]) Close() {
	iter.ator.Base().Close()
	iter.ator.Load(iter.ator.Over(), iter.ator.Base(), nil)
}

func (iter TxIter[Iter]) Valid() bool {
	return iter.ator.Valid()
}

func (iter TxIter[Iter]) Error() error {
	return iter.ator.Error()
}

func (iter TxIter[Iter]) Key() []byte {
	return iter.ator.Key()
}

func (iter TxIter[Iter]) Val() []byte {
	return iter.ator.Val()
}

func (iter TxIter[Iter]) Next() bool {
	return iter.ator.Next()
}

func (iter TxIter[Iter]) Prev() bool {
	return iter.ator.Prev()
}

func (iter TxIter[Iter]) SeekFirst() bool {
	return iter.ator.SeekFirst()
}

func (iter TxIter[Iter]) SeekLast() bool {
	return iter.ator.SeekLast()
}

func (iter TxIter[Iter]) Seek(key []byte) bool {
	return iter.ator.Seek(key)
}
