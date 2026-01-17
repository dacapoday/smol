package block

import (
	"sync"

	"github.com/dacapoday/smol/internal/heap"
)

// Heap implements Block
type Heap[F File] struct {
	pool sync.Pool
	heap heap.Heap[F]
}

func (block *Heap[F]) File() F {
	return block.heap.File()
}

func (block *Heap[F]) Load(file F, opt HeapOption) (entry []byte, ckpt HeapCheckpoint, err error) {
	meta, ckpt, err := block.heap.Load(file, opt)
	if err != nil {
		return
	}

	blockSize := int(meta.BlockSize)
	block.pool.New = func() any { return make([]byte, blockSize) }
	entry = meta.Entry
	return
}

func (block *Heap[F]) Close() error {
	block.pool.New = nil
	return block.heap.Close()
}

func (block *Heap[F]) Commit(entry []byte) (cp HeapCheckpoint, err error) {
	_, cp, err = block.heap.Commit(entry)
	return
}

func (block *Heap[F]) Rollback() error {
	return block.heap.Rollback()
}

func (block *Heap[F]) PageSize() int {
	return block.heap.PageSize()
}

func (block *Heap[F]) AllocateBuffer() []byte {
	return block.pool.Get().([]byte)
}

func (block *Heap[F]) RecycleBuffer(buffer []byte) {
	block.pool.Put(buffer)
}

func (block *Heap[F]) AllocateBlock() (blockID BlockID) {
	blockID, _ = block.heap.Allocate()
	return
}

func (block *Heap[F]) RecycleBlock(blockID BlockID) {
	block.heap.Recycle(blockID)
}

func (block *Heap[F]) Error() error {
	return block.heap.Error()
}

func (block *Heap[F]) LoadBlock(blockID BlockID) (buffer []byte, err error) {
	buffer = block.AllocateBuffer()
	if err = block.heap.ReadBlock(blockID, buffer); err != nil {
		block.RecycleBuffer(buffer)
		buffer = nil
	}
	return
}

func (block *Heap[F]) ReadBlock(blockID BlockID, buffer []byte, reader func(block []byte)) (err error) {
	if err = block.heap.ReadBlock(blockID, buffer); err != nil {
		return
	}
	if reader != nil {
		reader(buffer)
	}
	return
}

func (block *Heap[F]) WriteBlock(blockID BlockID, buffer []byte) (err error) {
	return block.heap.WriteBlock(blockID, buffer)
}

func (block *Heap[F]) BufferPressured(holding int) bool {
	return holding > 4096
}
