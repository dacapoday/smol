// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package heap

import (
	"errors"
	"fmt"
	"hash/crc32"
	"iter"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dacapoday/smol"
)

var castagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)

type BlockID = smol.BlockID
type File = smol.File

type Heap[F File] struct {
	block[F]
	free
	tail, base, head *checkpoint

	phase atomic.Pointer[phase]
	mutex sync.Mutex

	ckp    uint32
	metaID BlockID

	ignoreInvalidFreelist bool
}

type phase struct{ error }

var readwrite = &phase{errors.New("readwrite")}
var readyonly = &phase{errors.New("readonly")}

type Checkpoint = *checkpoint

type checkpoint struct {
	next     *checkpoint
	recycled uint32
	ref      atomic.Int32
}

var _ smol.Checkpoint = (*checkpoint)(nil)

func (ckpt *checkpoint) Acquire() {
	ckpt.ref.Add(1)
}

func (ckpt *checkpoint) Release() {
	ckpt.ref.Add(-1)
}

func (heap *Heap[F]) Load(file F, opt Option) (meta *Meta, ckpt Checkpoint, err error) {
	heap.mutex.Lock()
	defer heap.mutex.Unlock()

	if heap.block.opened() {
		err = fmt.Errorf("already %w", ErrOpened)
		return
	}

	magic := opt.MagicCode()
	meta, err = heap.block.load(file, magic)
	if err != nil {
		if opt.ReadOnly() || !errors.Is(err, ErrFileEmpty) {
			err = fmt.Errorf("heap.block.load failed: %w", err)
			return
		}

		var blockSize int
		if o, ok := opt.(BlockSize); ok {
			blockSize = o.BlockSize()
		} else {
			blockSize = os.Getpagesize()
		}
		if blockSize > 64*1024 || blockSize < 512 {
			err = fmt.Errorf("%d is %w", blockSize, ErrInvalidBlockSize)
			return
		}

		meta, err = heap.block.init(file, magic, uint32(blockSize))
		if err != nil {
			err = fmt.Errorf("heap.block.init failed: %w", err)
			return
		}
	} else if err = heap.loadEntry(meta); err != nil {
		meta = nil
		return
	}

	if opt.ReadOnly() {
		ckpt = new(checkpoint)
		ckpt.Acquire()
		heap.head = ckpt
		heap.phase.Store(readyonly)
		return
	}

	heap.ignoreInvalidFreelist = opt.IgnoreInvalidFreelist()
	heap.free.tail.capacity = freelistCapacity(heap.block.size)
	if err = restore(heap, meta); err != nil {
		meta = nil
		return
	}

	heap.head = nil
	countdown := int(opt.RetainCheckpoints())
	replay := func(recycled uint32) {
		ckpt := new(checkpoint)
		ckpt.recycled = recycled
		ckpt.next = heap.head
		heap.head = ckpt
		if countdown >= 0 {
			ckpt.Acquire()
			if countdown == 0 {
				heap.base = ckpt
			}
		}
		countdown--
	}

	replay(meta.FreeRecycled)
	heap.tail = heap.head
	heap.ckp = meta.Ckp
	heap.metaID = meta.ID
	ckpt = heap.tail
	ckpt.Acquire()

	rest := meta.FreeTotal
	prevID := meta.PrevID
	for prevID != 0 {
		if prevID == 1 {
			panic(errors.New("prevID == 1"))
		}

		meta, err := heap.meta(prevID)
		if err != nil {
			break
		}

		replay(min(meta.FreeRecycled, rest))

		if rest -= heap.head.recycled; rest == 0 {
			break
		}

		prevID = meta.PrevID
	}
	if rest > 0 {
		replay(rest)
	}
	for countdown >= 0 {
		replay(0)
	}

	heap.phase.Store(readwrite)
	if meta.EntryID > 1 {
		heap.recycle(meta.EntryID)
	}
	if meta.ID > 1 {
		heap.recycle(meta.ID)
	}
	return
}

func (heap *Heap[F]) AllCheckpointReleased() bool {
	if phase := heap.phase.Load(); phase == nil {
		return true
	}

	heap.mutex.Lock()
	defer heap.mutex.Unlock()

	ref := int32(0)
	cur := heap.head
	for cur != nil {
		if cur == heap.base {
			ref = 1
		}
		if cur.ref.Load() != ref {
			return false
		}
		cur = cur.next
	}
	return true
}

func (heap *Heap[F]) Close() error {
	phase := heap.phase.Swap(nil)
	if phase == nil {
		return nil
	}

	heap.mutex.Lock()
	defer heap.mutex.Unlock()

	heap.tail, heap.base, heap.head = nil, nil, nil
	heap.free = free{}
	return heap.block.close()
}

func (heap *Heap[F]) Extend() (blockID BlockID) {
	heap.mutex.Lock()
	blockID = heap.extend()
	heap.mutex.Unlock()
	return
}

func (heap *Heap[F]) extend() BlockID {
	if phase := heap.phase.Load(); phase != readwrite {
		return 0
	}

	blockID, err := heap.block.extend()
	if err != nil {
		heap.phase.CompareAndSwap(readwrite, &phase{error: err})
		return 0
	}
	return blockID
}

func (heap *Heap[F]) Allocate() (blockID BlockID, reuse bool) {
	heap.mutex.Lock()
	blockID, reuse = allocate(heap, heap.recycle)
	heap.mutex.Unlock()
	return
}

func (heap *Heap[F]) RecycleN(iter iter.Seq[BlockID]) {
	heap.mutex.Lock()
	for blockID := range iter {
		if blockID < 2 {
			panic(errors.New("blockID < 2"))
		}

		heap.recycle(blockID)
	}
	heap.mutex.Unlock()
}

func (heap *Heap[F]) Recycle(blockID BlockID) {
	if blockID < 2 {
		panic(errors.New("blockID < 2"))
	}

	heap.mutex.Lock()
	heap.recycle(blockID)
	heap.mutex.Unlock()
}

func (heap *Heap[F]) recycle(blockID BlockID) {
	ids := []BlockID{blockID}
	recycle := func(id BlockID) {
		ids = append(ids, id)
	}

	for len(ids) > 0 {
		blockID = ids[0]
		if !heap.free.tail.push(blockID) {
			freelistID, reuse := allocate(heap, recycle)
			if freelistID < 2 {
				return
			}
			if reuse && heap.free.tail.push(blockID) {
				if err := heap.commitFreelist(freelistID); err != nil {
					err = fmt.Errorf("write freelist(%d) failed: %w", freelistID, err)
					heap.phase.CompareAndSwap(readwrite, &phase{err})
					return
				}
			} else {
				if err := heap.commitFreelist(freelistID); err != nil {
					err = fmt.Errorf("write freelist(%d) failed: %w", freelistID, err)
					heap.phase.CompareAndSwap(readwrite, &phase{err})
					return
				}
				if !heap.free.tail.push(blockID) {
					panic(errors.New("!heap.free.tail.push(blockID)"))
				}
			}
		}
		heap.free.recycled++
		ids = ids[1:]
	}
}

func (heap *Heap[F]) ReadAt(buffer []byte, blockID BlockID) (int, error) {
	return heap.block.readAt(buffer, blockID)
}

func (heap *Heap[F]) WriteAt(buffer []byte, blockID BlockID) (n int, err error) {
	if blockID < 2 {
		panic(errors.New("blockID < 2"))
	}
	// blockSize := int(heap.block.size)
	// bufferSize := len(buffer)
	// if bufferSize > blockSize {
	// 	panic(errors.New("bufferSize > blockSize"))
	// }

	if phase := heap.phase.Load(); phase != readwrite {
		if phase == readyonly {
			err = ErrReadOnly
			return
		}
		if phase == nil {
			err = ErrClosed
			return
		}
		err = phase.error
		return
	}

	if n, err = heap.block.writeAt(buffer, blockID); err != nil {
		err = fmt.Errorf("write block(%d) failed: %w", blockID, err)
		heap.phase.CompareAndSwap(readwrite, &phase{err})
	}
	return
}

func (heap *Heap[F]) Error() (err error) {
	phase := heap.phase.Load()
	if phase == readwrite {
		return
	}
	if phase == readyonly {
		err = ErrReadOnly
		return
	}
	if phase == nil {
		err = ErrClosed
		return
	}
	err = phase.error
	return
}

func (heap *Heap[F]) Rollback() (err error) {
	heap.mutex.Lock()
	defer heap.mutex.Unlock()

	if phase := heap.phase.Load(); phase != readwrite {
		if phase == readyonly {
			return
		}
		if phase == nil {
			err = ErrClosed
			return
		}
	}

	meta, err := heap.block.meta(BlockID(heap.ckp % 2))
	if err != nil {
		return
	}

	rollback := meta.FreeRecycled + meta.FreeTotal - heap.free.total

	if err = restore(heap, meta); err != nil {
		return
	}

	heap.block.count = meta.BlockCount

	ckpt := new(checkpoint)
	ckpt.recycled = rollback
	ckpt.next = heap.head
	heap.head = ckpt

	if meta.EntryID > 1 {
		heap.recycle(meta.EntryID)
	}
	if meta.ID > 1 {
		heap.recycle(meta.ID)
	}
	return
}

func (heap *Heap[F]) Commit(entry []byte) (meta *Meta, ckpt Checkpoint, err error) {
	heap.mutex.Lock()
	defer heap.mutex.Unlock()

	if phase := heap.phase.Load(); phase != readwrite {
		if phase == readyonly {
			err = ErrReadOnly
			return
		}
		if phase == nil {
			err = ErrClosed
			return
		}
		err = phase.error
		return
	}

	meta = new(Meta)
	meta.UpdateTime = time.Now().UnixMilli()
	if heap.base != heap.tail {
		meta.ID, _ = allocate(heap, heap.recycle)
		if meta.ID < 2 {
			meta = nil
			err = heap.Error()
			return
		}
	}
	meta.PrevID = heap.metaID
	meta.BlockSize = uint32(heap.block.size)
	meta.Ckp = heap.ckp + 1

	defer func() {
		if err != nil {
			meta = nil
			return
		}

		heap.free.total += heap.free.recycled
		heap.free.recycled = 0

		ckpt = new(checkpoint)
		ckpt.recycled = meta.FreeRecycled
		ckpt.ref.Store(2)

		heap.tail.next = ckpt
		heap.tail = ckpt

		heap.base.ref.Add(-1)
		heap.base = heap.base.next

		heap.ckp = meta.Ckp
		heap.metaID = meta.ID

		if meta.EntryID > 1 {
			meta.Entry = entry
			heap.recycle(meta.EntryID)
		}
		if meta.ID > 1 {
			heap.recycle(meta.ID)
		}
	}()

	entrySize := len(entry)
	blockSize := int(heap.block.size)
	if entrySize > blockSize {
		panic(errors.New("entrySize > blockSize"))
	}
	meta.EntrySize = uint32(entrySize)

	meta.Entry = entry
	if blockSize >= entrySize+freelistSize(heap.free.tail.length)+74 {
		err = heap.save(meta)
		if !errors.Is(err, ErrOutOfRange) {
			if err != nil {
				err = fmt.Errorf("save meta(%d) failed: %w", meta.Ckp%2, err)
				heap.phase.CompareAndSwap(readwrite, &phase{err})
			}
			return
		}
	}

	if entrySize > blockSize-256 {
		meta.EntryID, _ = allocate(heap, heap.recycle)
		if meta.EntryID < 2 {
			err = heap.Error()
			return
		}

		meta.Entry, err = heap.writeEntry(meta.EntryID, meta.Entry)
		if err != nil {
			err = fmt.Errorf("write entry(%d) failed: %w", meta.EntryID, err)
			heap.phase.CompareAndSwap(readwrite, &phase{err})
			return
		}

		err = heap.save(meta)
		if !errors.Is(err, ErrOutOfRange) {
			if err != nil {
				err = fmt.Errorf("save meta(%d) failed: %w", meta.Ckp%2, err)
				heap.phase.CompareAndSwap(readwrite, &phase{err})
			}
			return
		}
	}

	{
		freelistID, _ := allocate(heap, heap.recycle)
		if freelistID < 2 {
			err = heap.Error()
			return
		}
		if err = heap.commitFreelist(freelistID); err != nil {
			err = fmt.Errorf("write freelist(%d) failed: %w", freelistID, err)
			heap.phase.CompareAndSwap(readwrite, &phase{err})
			return
		}
	}

	err = heap.save(meta)
	if err != nil {
		err = fmt.Errorf("save meta(%d) failed: %w", meta.Ckp%2, err)
		heap.phase.CompareAndSwap(readwrite, &phase{err})
	}
	return
}

func (heap *Heap[F]) save(meta *Meta) (err error) {
	meta.Freelist = heap.freelist()
	meta.FreeRecycled = heap.free.recycled
	meta.FreeTotal = heap.free.total
	meta.BlockCount = heap.block.count
	return heap.block.save(meta)
}

func (heap *Heap[F]) commitFreelist(blockID BlockID) (err error) {
	{
		freelist := heap.buffer
		ring2freelist(&heap.free.tail, heap.free.queue.bottom(), freelist)
		if _, err = heap.block.writeAt(freelist, blockID); err != nil {
			return
		}
	}
	heap.free.queue.push(blockID)
	if heap.free.head == &heap.free.tail {
		ring := heap.free.tail // copy
		heap.free.head = &ring
		heap.free.tail.buffer = nil // split ring
	}
	heap.free.tail.reset()
	return
}
