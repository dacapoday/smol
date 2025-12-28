// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package heap

import (
	"errors"
	"fmt"
)

type free struct {
	queue
	head            *ring
	tail            ring
	total, recycled uint32
}

func restore[F File](heap *Heap[F], meta *Meta) (err error) {
	var free free
	free.tail.capacity = heap.free.tail.capacity
	free.tail.reset()
	free.head = &free.tail
	defer func() {
		if err != nil {
			return
		}

		{ // copy
			heap.free = free
			if free.head == &free.tail {
				heap.free.head = &heap.free.tail
			}
		}
	}()

	free.total = meta.FreeRecycled + meta.FreeTotal
	if free.total == 0 {
		return
	}

	freelist := meta.Freelist
	if freelist.invalid() {
		if heap.ignoreInvalidFreelist {
			meta.FreeRecycled = 0
			meta.FreeTotal = 0
			free.total = 0
			return
		}
		err = fmt.Errorf("meta.Freelist is %w", ErrInvalidFreelist)
		return
	}

	count := freelist.Count()
	freelist2ring(freelist, &free.tail, count)

	total := uint32(count)
	if total >= free.total {
		if total > free.total {
			panic(errors.New("total > free.total"))
		}
		return
	}

	prev := freelist.Prev()
	load := func(blockID BlockID) (Freelist, error) {
		freelist := Freelist(heap.buffer)
		if _, err := heap.block.readAt(freelist, blockID); err != nil {
			return nil, fmt.Errorf("read freelist(%v) failed: %w", blockID, err)
		}
		if freelist.invalid() {
			return nil, fmt.Errorf("block(%v) is %w", blockID, ErrInvalidFreelist)
		}
		return freelist, nil
	}
	for {
		if freelist, err = load(prev); err != nil {
			break
		}

		free.queue.unshift(prev)

		count = freelist.Count()
		if total+uint32(count) >= free.total {
			count = uint16(free.total - total)
			break
		}

		total += uint32(count)
		prev = freelist.Prev()
	}
	if err != nil {
		if !heap.ignoreInvalidFreelist {
			return
		}
		err = nil

		if prev = free.queue.top(); prev != 0 {
			if freelist, err = load(prev); err != nil {
				return
			}
		}

		if meta.FreeRecycled > total {
			meta.FreeRecycled = total
			meta.FreeTotal = 0
		} else {
			meta.FreeTotal = total - meta.FreeRecycled
		}
		free.total = total
	}
	if freelist != nil {
		ring := free.tail
		ring.buffer = nil
		ring.reset()
		freelist2ring(freelist, &ring, count)
		free.head = &ring
	}
	return
}

func allocate[F File](heap *Heap[F], recycle func(BlockID)) (blockID BlockID, reuse bool) {
	for heap.head != nil && heap.head.ref.Load() <= 0 {
		if heap.head.recycled == 0 {
			heap.head = heap.head.next
			continue
		}

		if blockID = heap.free.head.shift(); blockID != 0 {
			heap.head.recycled--
			heap.free.total--
			reuse = true
			return
		}

		prevID := heap.free.queue.shift()
		if nextID := heap.free.queue.top(); nextID == 0 {
			if heap.free.head == &heap.free.tail {
				panic(errors.New("heap.free.head == &heap.free.tail"))
			}
			heap.free.head = &heap.free.tail
		} else {
			freelist := Freelist(heap.buffer)
			if _, err := heap.block.readAt(freelist, nextID); err != nil {
				if prevID != 0 {
					heap.free.queue.unshift(prevID)
				}
				heap.phase.CompareAndSwap(readwrite, &phase{error: fmt.Errorf("read freelist(%v) failed: %w", nextID, err)})
				break
			}

			if freelist.invalid() {
				if prevID != 0 {
					heap.free.queue.unshift(prevID)
				}
				heap.phase.CompareAndSwap(readwrite, &phase{error: fmt.Errorf("block(%v) is %w", nextID, ErrInvalidFreelist)})
				break
			}

			// heap.free.head.reset()
			freelist2ring(freelist, heap.free.head, freelist.Count())
		}
		if prevID != 0 {
			defer recycle(prevID)
		}
	}
	return heap.extend(), false
}

func (free *free) freelist() (freelist Freelist) {
	freelist = make([]byte, freelistSize(free.tail.length))
	ring2freelist(&free.tail, free.queue.bottom(), freelist)
	return
}

type ring struct {
	buffer           []BlockID
	capacity, length uint16
	head, tail       uint16
}

func (ring *ring) reset() {
	ring.length = 0
	ring.head = 0
	ring.tail = 0
	if ring.buffer == nil {
		ring.buffer = make([]BlockID, ring.capacity)
	}
}

func (ring *ring) full() bool {
	return ring.length == ring.capacity
}

func (ring *ring) empty() bool {
	return ring.length == 0
}

func (ring *ring) top() (id BlockID) {
	if ring.empty() {
		return
	}

	return ring.buffer[ring.head]
}

func (ring *ring) bottom() (id BlockID) {
	if ring.empty() {
		return
	}

	return ring.buffer[(ring.tail-1+ring.capacity)%ring.capacity]
}

func (ring *ring) shift() (id BlockID) {
	if ring.empty() {
		return
	}

	id = ring.buffer[ring.head]
	ring.head = (ring.head + 1) % ring.capacity
	ring.length--
	return
}

func (ring *ring) push(id BlockID) bool {
	// if id < 2 {
	// 	panic(errors.New("id < 2"))
	// }

	if ring.full() {
		return false
	}

	ring.buffer[ring.tail] = id
	ring.tail = (ring.tail + 1) % ring.capacity
	ring.length++
	return true
}

func (ring *ring) unshift(id BlockID) bool {
	// if id < 2 {
	// 	panic(errors.New("id < 2"))
	// }

	if ring.full() {
		return false
	}

	ring.head = (ring.head - 1 + ring.capacity) % ring.capacity
	ring.buffer[ring.head] = id
	ring.length++
	return true
}

func (ring *ring) freelist(yield func(i uint16, id uint32) bool) {
	index := ring.head
	for i := range ring.length {
		if !yield(ring.length-i-1, uint32(ring.buffer[index])) {
			return
		}
		index = (index + 1) % ring.capacity
	}
}

type queue struct {
	head, tail *qnode
}

type qnode struct {
	ring
	next *qnode
}

func node(capacity uint16) *qnode {
	return &qnode{ring: ring{
		capacity: capacity,
		buffer:   make([]BlockID, capacity),
	}}
}

func (q *queue) top() (id BlockID) {
	if q.head != nil {
		id = q.head.top()
		if id == 0 {
			if q.head.next != nil {
				id = q.head.next.top()
			}
		}
	}
	return
}

func (q *queue) bottom() BlockID {
	if q.tail == nil {
		return 0
	}
	return q.tail.bottom()
}

func (q *queue) shift() (id BlockID) {
	if q.head == nil {
		q.tail = nil
		return
	}
	id = q.head.shift()
	if id == 0 {
		q.head.buffer = nil
		q.head = q.head.next
		id = q.shift()
		return
	}
	return
}

func (q *queue) push(id BlockID) {
	if q.tail == nil {
		q.tail = node(4)
		q.head = q.tail
		q.tail.push(id)
		return
	}

	if !q.tail.push(id) {
		tail := node(min(q.head.capacity*2, 1024))
		q.tail.next = tail
		q.tail = tail
		q.tail.push(id)
		return
	}
}

func (q *queue) unshift(id BlockID) {
	if q.head == nil {
		q.tail = node(4)
		q.head = q.tail
		q.tail.push(id)
		return
	}

	if !q.head.unshift(id) {
		head := node(min(q.head.capacity*2, 1024))
		head.next = q.head
		q.head = head
		q.head.push(id)
		return
	}
}
