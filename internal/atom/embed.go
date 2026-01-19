package atom

import (
	"sync"

	"github.com/dacapoday/smol"
)

type Block[T any, C Checkpoint] interface {
	*T
	smol.Block[C]
}

type Embed[T any, B Block[T, C], C Checkpoint, V any] struct {
	block T
	ckpt  C
	val   V
	view  sync.RWMutex
	mutex sync.Mutex
}

func (atom *Embed[T, B, C, V]) Block() B {
	return B(&atom.block)
}

func (atom *Embed[T, B, C, V]) Load(ckpt C, val V) {
	atom.mutex.Lock()
	atom.view.Lock()
	// var nilCkpt C
	// if atom.ckpt != nilCkpt {
	// 	atom.ckpt.Release()
	// }
	atom.ckpt, atom.val = ckpt, val
	atom.view.Unlock()
	atom.mutex.Unlock()
}

func (atom *Embed[T, B, C, V]) Close() (err error) {
	atom.mutex.Lock()
	defer atom.mutex.Unlock()
	atom.view.Lock()
	defer atom.view.Unlock()

	var nilCkpt C
	if atom.ckpt == nilCkpt {
		return
	}
	atom.ckpt.Release()
	atom.ckpt = nilCkpt

	var nilVal V
	atom.val = nilVal

	return atom.Block().Close()
}

func (atom *Embed[T, B, C, V]) Acquire() (val V, ckpt C) {
	atom.view.RLock()
	var nilCkpt C
	if ckpt = atom.ckpt; ckpt != nilCkpt {
		ckpt.Acquire()
		val = atom.val
	}
	atom.view.RUnlock()
	return
}

func (atom *Embed[T, B, C, V]) Swap(swap func(block B, val V) (entry []byte, newVal V, err error)) (err error) {
	atom.mutex.Lock()
	defer atom.mutex.Unlock()

	oldCkpt := atom.ckpt
	var nilCkpt C
	if oldCkpt == nilCkpt {
		return ErrClosed
	}

	block := atom.Block()
	entry, newVal, err := swap(block, atom.val)
	if err != nil {
		block.Rollback()
		return
	}

	newCkpt, err := block.Commit(entry)
	if err != nil {
		return
	}

	atom.view.Lock()
	atom.val = newVal
	atom.ckpt = newCkpt
	oldCkpt.Release()
	atom.view.Unlock()
	return
}
