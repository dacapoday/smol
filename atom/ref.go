// Package atom provides atomic state containers for COW data structures.
//
// Manages the lifecycle of Block, Checkpoint, and derived value as a unit.
// Supports concurrent reads (Acquire) and serialized writes (Swap).
//
// Two container variants:
//   - Ref: references external Block (for shared ownership)
//   - Own: embeds Block value (for exclusive ownership)
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

// Ref holds a reference to external Block with its Checkpoint and value.
// For shared Block ownership scenarios.
//
// Type parameters:
//   - B: Block type (must be pointer/interface)
//   - C: Checkpoint type
//   - V: Value type (typically deserialized entry)
//
// Zero value is closed. Call Load to initialize.
type Ref[B smol.Block[C], C Checkpoint, V any] struct {
	block B
	ckpt  C
	val   V
	view  sync.RWMutex
	mutex sync.Mutex
}

// Block returns the underlying Block.
func (ref *Ref[B, C, V]) Block() B {
	return ref.block
}

// Load initializes Ref with block, checkpoint, and value.
// Replaces any existing state without releasing old checkpoint.
func (ref *Ref[B, C, V]) Load(block B, ckpt C, val V) {
	ref.mutex.Lock()
	ref.view.Lock()
	// var nilCkpt C
	// if atom.ckpt != nilCkpt {
	// 	atom.ckpt.Release()
	// }
	ref.block, ref.ckpt, ref.val = block, ckpt, val
	ref.view.Unlock()
	ref.mutex.Unlock()
}

// Close releases checkpoint and closes Block.
// No-op if already closed.
func (ref *Ref[B, C, V]) Close() (err error) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.view.Lock()
	defer ref.view.Unlock()

	var nilCkpt C
	if ref.ckpt == nilCkpt {
		return
	}
	ref.ckpt.Release()
	ref.ckpt = nilCkpt

	var nilVal V
	ref.val = nilVal

	return ref.block.Close()
}

// Acquire returns current value and acquired checkpoint for reading.
// Returns zero values if closed.
//
// Important: Caller must call ckpt.Release() when done.
func (ref *Ref[B, C, V]) Acquire() (val V, ckpt C) {
	ref.view.RLock()
	var nilCkpt C
	if ckpt = ref.ckpt; ckpt != nilCkpt {
		ckpt.Acquire()
		val = ref.val
	}
	ref.view.RUnlock()
	return
}

// Swap atomically updates value via transaction.
//
// The swap function receives current block and value, returns:
//   - entry: serialized state for Commit
//   - newVal: new value after COW derivation
//   - err: non-nil triggers Rollback
//
// On success, commits entry, switches to newVal, releases old checkpoint.
// On error, rolls back Block changes.
func (ref *Ref[B, C, V]) Swap(swap func(block B, val V) (entry []byte, newVal V, err error)) (err error) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()

	oldCkpt := ref.ckpt
	var nilCkpt C
	if oldCkpt == nilCkpt {
		return ErrClosed
	}

	entry, newVal, err := swap(ref.block, ref.val)
	if err != nil {
		ref.block.Rollback()
		return
	}

	newCkpt, err := ref.block.Commit(entry)
	if err != nil {
		return
	}

	ref.view.Lock()
	ref.val = newVal
	ref.ckpt = newCkpt
	oldCkpt.Release()
	ref.view.Unlock()
	return
}
