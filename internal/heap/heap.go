// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package heap

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dacapoday/smol"
)

type BlockID = smol.BlockID
type File = smol.File

type Heap[F File] struct {
	codec
	block[F]
	buffer []byte

	free
	tail, base, head *checkpoint

	phase atomic.Pointer[phase]
	mutex sync.Mutex

	ckp    uint32
	magic  [4]byte
	metaID BlockID

	ignoreInvalidFreelist bool
}

type phase struct{ error }

var readwrite = &phase{errors.New("readwrite")}
var readonly = &phase{errors.New("readonly")}

type Checkpoint = *checkpoint

type checkpoint struct {
	next     *checkpoint
	recycled uint32
	ref      atomic.Int32
}

var _ smol.Checkpoint = (*checkpoint)(nil)

func (ckpt *checkpoint) Acquire() {
	ckpt.ref.Add(1) // TODO: check overflow
}

func (ckpt *checkpoint) Release() {
	ckpt.ref.Add(-1)
}

func (ckpt *checkpoint) Valid() bool {
	return ckpt.ref.Load() > 1
}

func (heap *Heap[F]) PageSize() int {
	return heap.BlockSize() - heap.codec.size()
}

func (heap *Heap[F]) Load(file F, opt Option) (meta *Meta, ckpt Checkpoint, err error) {
	heap.mutex.Lock()
	defer heap.mutex.Unlock()

	if heap.phase.Load() != nil {
		panic("heap.Load: already open")
	}

	meta, err = heap.load(file, opt)
	if err != nil {
		err = fmt.Errorf("heap.Load: %w", err)
		heap.phase.Store(&phase{error: err})
		return
	}

	if opt.ReadOnly() {
		ckpt = new(checkpoint)
		ckpt.Acquire()
		heap.head = ckpt
		heap.phase.Store(readonly)
		return
	}

	heap.ignoreInvalidFreelist = opt.IgnoreInvalidFreelist()
	heap.free.tail.capacity = freelistCapacity(heap.block.size)
	if err = heap.restore(meta); err != nil {
		meta = nil
		err = fmt.Errorf("heap.Load: %w", err)
		heap.phase.Store(&phase{error: err})
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

	for cur := heap.head; cur != nil; cur = cur.next {
		cur.ref.Store(0)
	}
	heap.tail, heap.base, heap.head = nil, nil, nil
	heap.free = free{}
	heap.codec = codec{}
	heap.buffer = nil
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
		err = fmt.Errorf("heap.extend: %w", err)
		heap.phase.CompareAndSwap(readwrite, &phase{error: err})
		return 0
	}
	return blockID
}

func (heap *Heap[F]) Allocate() (blockID BlockID, reuse bool) {
	heap.mutex.Lock()
	blockID, reuse = heap.allocate(heap.recycle)
	heap.mutex.Unlock()
	return
}

func (heap *Heap[F]) RecycleN(iter func(yield func(BlockID) bool)) {
	heap.mutex.Lock()
	for blockID := range iter {
		assertBlockID("heap.RecycleN", blockID)
		heap.recycle(blockID)
	}
	heap.mutex.Unlock()
}

func (heap *Heap[F]) Recycle(blockID BlockID) {
	assertBlockID("heap.Recycle", blockID)
	heap.mutex.Lock()
	heap.recycle(blockID)
	heap.mutex.Unlock()
}

func (heap *Heap[F]) ReadBlock(blockID BlockID, buffer []byte) (err error) {
	if phase := heap.phase.Load(); phase != readwrite {
		if phase == nil {
			err = ErrClosed
			return
		}
		err = phase.error
		return
	}

	if _, err = heap.block.readAt(buffer, blockID); err != nil {
		return
	}
	return heap.codec.decode(buffer, blockID)
}

func (heap *Heap[F]) ReadAt(buffer []byte, blockID BlockID) (n int, err error) {
	if phase := heap.phase.Load(); phase != readwrite {
		if phase == nil {
			err = ErrClosed
			return
		}
		err = phase.error
		return
	}

	return heap.block.readAt(buffer, blockID)
}

func (heap *Heap[F]) WriteBlock(blockID BlockID, buffer []byte) (err error) {
	assertBlockID("heap.WriteBlock", blockID)
	if phase := heap.phase.Load(); phase != readwrite {
		if phase == readonly {
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

	heap.codec.encode(buffer, blockID)

	if _, err = heap.block.writeAt(buffer, blockID); err != nil {
		err = fmt.Errorf("heap.WriteBlock(%d): %w", blockID, err)
		heap.phase.CompareAndSwap(readwrite, &phase{err})
	}
	return
}

func (heap *Heap[F]) WriteAt(buffer []byte, blockID BlockID) (n int, err error) {
	assertBlockID("heap.WriteAt", blockID)
	if phase := heap.phase.Load(); phase != readwrite {
		if phase == readonly {
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
		err = fmt.Errorf("heap.WriteAt(%d): %w", blockID, err)
		heap.phase.CompareAndSwap(readwrite, &phase{err})
	}
	return
}

func (heap *Heap[F]) Error() (err error) {
	phase := heap.phase.Load()
	if phase == readwrite {
		return
	}
	if phase == readonly {
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
		if phase == readonly {
			return
		}
		if phase == nil {
			err = ErrClosed
			return
		}
	}

	meta, err := heap.meta(BlockID(heap.ckp % 2))
	if err != nil {
		err = fmt.Errorf("heap.Rollback: %w", err)
		return
	}

	rollback := meta.FreeRecycled + meta.FreeTotal - heap.free.total

	if err = heap.restore(meta); err != nil {
		err = fmt.Errorf("heap.Rollback: %w", err)
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
		if phase == readonly {
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

	entrySize := len(entry)
	if heap.codec.spec == nil {
		assertEntrySize("heap.Commit", entrySize, heap.BlockSize())
	} else {
		assertEntrySize("heap.Commit", entrySize, heap.PageSize())
	}

	meta = new(Meta)
	meta.UpdateTime = time.Now().UnixMilli()
	if heap.base != heap.tail {
		meta.ID, _ = heap.allocate(heap.recycle)
		if meta.ID < 2 {
			meta = nil
			err = heap.Error()
			return
		}
	}
	meta.PrevID = heap.metaID
	meta.BlockSize = uint32(heap.block.size)
	meta.CodecSpec = heap.codec.spec
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

		meta.Entry = entry
		if meta.EntryID > 1 {
			heap.recycle(meta.EntryID)
		}
		if meta.ID > 1 {
			heap.recycle(meta.ID)
		}
	}()

	meta.Entry = heap.codec.encodeEntry(entry)

	blockSize := int(heap.block.size)
	if blockSize >= len(meta.Entry)+freelistSize(heap.free.tail.length)+74 {
		if err = heap.save(meta); !errors.Is(err, errOutOfRange) {
			if err != nil {
				err = fmt.Errorf("heap.Commit: save meta(%d) failed: %w", meta.Ckp%2, err)
				heap.phase.CompareAndSwap(readwrite, &phase{err})
			}
			return
		}
	}

	if len(meta.Entry) > blockSize-256 {
		if meta.EntryID, _ = heap.allocate(heap.recycle); meta.EntryID < 2 {
			err = heap.Error()
			return
		}

		if err = heap.saveEntry(meta); err != nil {
			err = fmt.Errorf("heap.Commit: save entry(%d) failed: %w", meta.EntryID, err)
			heap.phase.CompareAndSwap(readwrite, &phase{err})
			return
		}

		if err = heap.save(meta); !errors.Is(err, errOutOfRange) {
			if err != nil {
				err = fmt.Errorf("heap.Commit: save meta(%d) failed: %w", meta.Ckp%2, err)
				heap.phase.CompareAndSwap(readwrite, &phase{err})
			}
			return
		}
	}

	{
		freelistID, _ := heap.allocate(heap.recycle)
		if freelistID < 2 {
			err = heap.Error()
			return
		}

		if err = heap.saveFreelist(freelistID); err != nil {
			err = fmt.Errorf("heap.Commit: save freelist(%d) failed: %w", freelistID, err)
			heap.phase.CompareAndSwap(readwrite, &phase{err})
			return
		}
	}

	if err = heap.save(meta); err != nil {
		err = fmt.Errorf("heap.Commit: save meta(%d) failed: %w", meta.Ckp%2, err)
		heap.phase.CompareAndSwap(readwrite, &phase{err})
	}
	return
}
