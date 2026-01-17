package atom

import (
	"sync"

	"github.com/dacapoday/smol"
)

type Block[T any, C Checkpoint] interface {
	*T
	smol.Block[C]
}

type Own[T any, B Block[T, C], C Checkpoint, V any] struct {
	block T
	ckpt  C
	val   V
	view  sync.RWMutex
	mutex sync.Mutex
}

func (own *Own[T, B, C, V]) Block() B {
	return B(&own.block)
}

func (own *Own[T, B, C, V]) Load(ckpt C, val V) {
	own.mutex.Lock()
	own.view.Lock()
	own.ckpt, own.val = ckpt, val
	own.view.Unlock()
	own.mutex.Unlock()
}

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

	err = own.Block().Close()
	return
}

func (own *Own[T, B, C, V]) Acquire() (ckpt C, val V) {
	own.view.RLock()
	var nilCkpt C
	if ckpt = own.ckpt; ckpt != nilCkpt {
		ckpt.Acquire()
		val = own.val
	}
	own.view.RUnlock()
	return
}

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
