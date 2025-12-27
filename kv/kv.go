package kv

import (
	"os"

	"github.com/dacapoday/smol/block"
	"github.com/dacapoday/smol/bptree"
)

type DB = KV[*os.File]

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

type KV[F File] struct {
	bptree bptree.BPTree[*block.CRC32Heap[F], block.HeapCheckpoint]
	block  block.CRC32Heap[F]
}

func (kv *KV[F]) File() F {
	return kv.block.File()
}

func (kv *KV[F]) Load(file F) (err error) {
	entry, ckpt, err := kv.block.Load(file, opt{})
	if err != nil {
		return
	}

	err = kv.bptree.Load(&kv.block, entry, ckpt)
	return
}

type opt struct{}

func (o opt) MagicCode() [4]byte {
	return [4]byte{'D', 'I', 'C', 'T'}
}

func (o opt) ReadOnly() bool {
	return false
}

func (o opt) IgnoreInvalidFreelist() bool {
	return false
}

func (o opt) RetainCheckpoints() uint8 {
	return 0
}

func (o opt) BlockSize() int {
	return 1 << 14
}

func (kv *KV[F]) Close() (err error) {
	return kv.bptree.Close()
}

func (kv *KV[F]) Get(key []byte) (val []byte, err error) {
	return kv.bptree.Get(key)
}

func (kv *KV[F]) Set(key []byte, val []byte) (err error) {
	return kv.bptree.Set(key, val)
}

func (kv *KV[F]) Batch(sortedChanges func(yield func([]byte, []byte) bool)) error {
	return kv.bptree.WriteSortedChanges(sortedChanges, 4096)
}
