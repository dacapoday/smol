// Package atom provides atomic state container for COW data structures.
//
// Manages the lifecycle of Checkpoint and derived value as a unit.
// Supports concurrent reads (Acquire) and serialized writes (Swap).
//
// Block is managed externally, allowing flexible ownership patterns.
//
// Naming: Inspired by Clojure's atom with region-based memory semantics.
// Value exists within Checkpoint region; region release invalidates value.
package atom

import (
	"sync"

	"github.com/dacapoday/smol"
)

// Checkpoint extends smol.Checkpoint with comparable constraint.
type Checkpoint = interface {
	comparable
	smol.Checkpoint
}

// Atom manages checkpoint and value, with Block managed externally.
//
// Type parameters:
//   - V: Value type
//   - C: Checkpoint type
//
// Zero value is closed. Call Load to initialize.
type Atom[V any, C Checkpoint] struct {
	val   V
	ckpt  C
	view  sync.RWMutex
	mutex sync.Mutex
}

// Load initializes Atom with value and checkpoint.
// Replaces any existing state without releasing old checkpoint.
func (a *Atom[V, C]) Load(val V, ckpt C) {
	a.mutex.Lock()
	a.view.Lock()
	a.val, a.ckpt = val, ckpt
	a.view.Unlock()
	a.mutex.Unlock()
}

// Close releases checkpoint.
// No-op if already closed.
func (a *Atom[V, C]) Close() {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.view.Lock()
	defer a.view.Unlock()

	var nilCkpt C
	if a.ckpt == nilCkpt {
		return
	}
	a.ckpt.Release()
	a.ckpt = nilCkpt

	var nilVal V
	a.val = nilVal
}

// Acquire returns current value and acquired checkpoint for reading.
// Returns zero values if closed.
//
// Important: Caller must call ckpt.Release() when done.
func (a *Atom[V, C]) Acquire() (val V, ckpt C) {
	a.view.RLock()
	var nilCkpt C
	if ckpt = a.ckpt; ckpt != nilCkpt {
		ckpt.Acquire()
		val = a.val
	}
	a.view.RUnlock()
	return
}

// Swap atomically updates value and checkpoint via callback.
//
// The swap function receives current value, returns:
//   - newVal: new value
//   - newCkpt: new checkpoint (caller responsible for creating)
//   - err: non-nil aborts the swap
//
// On success, switches to new state, releases old checkpoint.
// On error, state unchanged.
func (a *Atom[V, C]) Swap(swap func(val V) (newVal V, newCkpt C, err error)) (err error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	oldCkpt := a.ckpt
	var nilCkpt C
	if oldCkpt == nilCkpt {
		return ErrClosed
	}

	newVal, newCkpt, err := swap(a.val)
	if err != nil {
		return
	}

	a.view.Lock()
	a.val = newVal
	a.ckpt = newCkpt
	a.view.Unlock()

	oldCkpt.Release()
	return
}
