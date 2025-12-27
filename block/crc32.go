package block

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/dacapoday/smol/internal/heap"
)

type CRC32HeapOption interface {
	MagicCode() [4]byte
	ReadOnly() bool
	IgnoreInvalidFreelist() bool
	RetainCheckpoints() uint8
}

// CRC32Heap implements the block interface with CRC32 checksum protection
// for each block.
type CRC32Heap[F File] struct {
	pool sync.Pool
	heap heap.Heap[F]
	size int
}

func (block *CRC32Heap[F]) File() F {
	return block.heap.File()
}

func (block *CRC32Heap[F]) Load(file F, opt CRC32HeapOption) (entry []byte, ckpt HeapCheckpoint, err error) {
	meta, ckpt, err := block.heap.Load(file, opt)
	if err != nil {
		return
	}

	blockSize := int(meta.BlockSize)
	block.pool.New = func() any { return make([]byte, blockSize) }
	block.size = blockSize - 4
	entry = meta.Entry
	return
}

func (block *CRC32Heap[F]) Close() error {
	block.size = 0
	block.pool.New = nil
	return block.heap.Close()
}

func (block *CRC32Heap[F]) Commit(entry []byte) (cp HeapCheckpoint, err error) {
	_, cp, err = block.heap.Commit(entry)
	return
}

func (block *CRC32Heap[F]) Rollback() error {
	return block.heap.Rollback()
}

func (block *CRC32Heap[F]) PageSize() int {
	return block.size
}

func (block *CRC32Heap[F]) AllocateBuffer() []byte {
	return block.pool.Get().([]byte)
}

func (block *CRC32Heap[F]) RecycleBuffer(buffer []byte) {
	block.pool.Put(buffer)
}

func (block *CRC32Heap[F]) AllocateBlock() (blockID BlockID) {
	blockID, _ = block.heap.Allocate()
	return
}

func (block *CRC32Heap[F]) RecycleBlock(blockID BlockID) {
	block.heap.Recycle(blockID)
}

func (block *CRC32Heap[F]) Error() error {
	return block.heap.Error()
}

func (block *CRC32Heap[F]) LoadBlock(blockID BlockID) (buffer []byte, err error) {
	buffer = block.AllocateBuffer()
	if err = block.ReadBlock(blockID, buffer, nil); err != nil {
		block.RecycleBuffer(buffer)
		buffer = nil
	}
	return
}

func (block *CRC32Heap[F]) ReadBlock(blockID BlockID, buffer []byte, reader func(block []byte)) (err error) {
	if _, err = block.heap.ReadAt(buffer, blockID); err != nil {
		err = fmt.Errorf("read block(%v) failed: %w", blockID, err)
		return
	}

	sum := binary.LittleEndian.Uint32(buffer[block.size:])
	chksum := checksum(buffer[:block.size])
	if sum != chksum {
		err = fmt.Errorf("block(%v) has %w", blockID, ErrInvalidChecksum)
		return
	}

	if reader != nil {
		reader(buffer)
	}
	return
}

func (block *CRC32Heap[F]) WriteBlock(blockID BlockID, buffer []byte) (err error) {
	binary.LittleEndian.PutUint32(buffer[block.size:], checksum(buffer[:block.size]))
	_, err = block.heap.WriteAt(buffer, blockID)
	return
}
