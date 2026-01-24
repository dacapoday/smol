package atom

import (
	"sync"

	"github.com/dacapoday/smol"
)

// Block constrains T to be a value type whose pointer implements smol.Block.
type Block[T any, C Checkpoint] interface {
	*T
	smol.Block[C]
}

// Own embeds Block value with its Checkpoint and derived value.
// For exclusive Block ownership scenarios where Block lifecycle
// is bound to the container.
//
// Type parameters:
//   - T: Block value type (e.g., block.Heap[F])
//   - B: Block pointer type (*T, must implement smol.Block[C])
//   - C: Checkpoint type
//   - V: Value type (typically deserialized entry)
//
// Zero value has uninitialized Block. Call Block().Load() then Load().
type Own[T any, B Block[T, C], C Checkpoint, V any] struct {
	block T
	ckpt  C
	val   V
	view  sync.RWMutex
	mutex sync.Mutex
}

// Block returns pointer to embedded Block.
func (own *Own[T, B, C, V]) Block() B {
	return B(&own.block)
}

// Load initializes Own with checkpoint and value.
// Block must be initialized separately via Block().Load().
// Replaces any existing state without releasing old checkpoint.
func (own *Own[T, B, C, V]) Load(ckpt C, val V) {
	own.mutex.Lock()
	own.view.Lock()
	// var nilCkpt C
	// if atom.ckpt != nilCkpt {
	// 	atom.ckpt.Release()
	// }
	own.ckpt, own.val = ckpt, val
	own.view.Unlock()
	own.mutex.Unlock()
}

// Close releases checkpoint and closes embedded Block.
// No-op if already closed.
func (own *Own[T, B, C, V]) Close() (err error) {
	own.mutex.Lock()
	defer own.mutex.Unlock()
	own.view.Lock()
	defer own.view.Unlock()

	var nilCkpt C
	if own.ckpt == nilCkpt {
		return
	}
	own.ckpt.Release()
	own.ckpt = nilCkpt

	var nilVal V
	own.val = nilVal

	return own.Block().Close()
}

// Acquire returns current value and acquired checkpoint for reading.
// Returns zero values if closed.
//
// Important: Caller must call ckpt.Release() when done.
func (own *Own[T, B, C, V]) Acquire() (val V, ckpt C) {
	own.view.RLock()
	var nilCkpt C
	if ckpt = own.ckpt; ckpt != nilCkpt {
		ckpt.Acquire()
		val = own.val
	}
	own.view.RUnlock()
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
func (own *Own[T, B, C, V]) Swap(swap func(block B, val V) (entry []byte, newVal V, err error)) (err error) {
	own.mutex.Lock()
	defer own.mutex.Unlock()

	oldCkpt := own.ckpt
	var nilCkpt C
	if oldCkpt == nilCkpt {
		return ErrClosed
	}

	block := own.Block()
	entry, newVal, err := swap(block, own.val)
	if err != nil {
		block.Rollback()
		return
	}

	newCkpt, err := block.Commit(entry)
	if err != nil {
		return
	}

	own.view.Lock()
	own.val = newVal
	own.ckpt = newCkpt
	oldCkpt.Release()
	own.view.Unlock()
	return
}
