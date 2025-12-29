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

// Iter creates a new iterator over the key-value store.
// Captures a consistent snapshot at the current moment.
//
// Important: Caller must call Close to release resources.
func (kv *KV[F]) Iter() (iter Iter[F]) {
	iter.ator = kv.bptree.Iterator()
	return
}

// Iter is an iterator over a KV store snapshot.
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
