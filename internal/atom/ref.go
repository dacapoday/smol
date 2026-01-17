package atom

import (
	"sync"

	"github.com/dacapoday/smol"
)

type Checkpoint = interface {
	comparable
	smol.Checkpoint
}

type Ref[B smol.Block[C], C Checkpoint, V any] struct {
	block B
	ckpt  C
	val   V
	view  sync.RWMutex
	mutex sync.Mutex
}

func (ref *Ref[B, C, V]) Block() B {
	return ref.block
}

func (ref *Ref[B, C, V]) Load(block B, ckpt C, val V) {
	ref.mutex.Lock()
	ref.view.Lock()
	ref.block, ref.ckpt, ref.val = block, ckpt, val
	ref.view.Unlock()
	ref.mutex.Unlock()
}

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

	err = ref.block.Close()
	var nilBlock B
	ref.block = nilBlock
	return
}

func (ref *Ref[B, C, V]) Acquire() (ckpt C, val V) {
	ref.view.RLock()
	var nilCkpt C
	if ckpt = ref.ckpt; ckpt != nilCkpt {
		ckpt.Acquire()
		val = ref.val
	}
	ref.view.RUnlock()
	return
}

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
