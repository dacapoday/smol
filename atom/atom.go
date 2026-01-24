package atom

import (
	"sync"

	"github.com/dacapoday/smol"
)

type Checkpoint = interface {
	comparable
	smol.Checkpoint
}

type Atom[B smol.Block[C], C Checkpoint, V any] struct {
	block B
	ckpt  C
	val   V
	view  sync.RWMutex
	mutex sync.Mutex
}

func (atom *Atom[B, C, V]) Block() B {
	return atom.block
}

func (atom *Atom[B, C, V]) Load(block B, ckpt C, val V) {
	atom.mutex.Lock()
	atom.view.Lock()
	// var nilCkpt C
	// if atom.ckpt != nilCkpt {
	// 	atom.ckpt.Release()
	// }
	atom.block, atom.ckpt, atom.val = block, ckpt, val
	atom.view.Unlock()
	atom.mutex.Unlock()
}

func (atom *Atom[B, C, V]) Close() (err error) {
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

	return atom.block.Close()
}

func (atom *Atom[B, C, V]) Acquire() (val V, ckpt C) {
	atom.view.RLock()
	var nilCkpt C
	if ckpt = atom.ckpt; ckpt != nilCkpt {
		ckpt.Acquire()
		val = atom.val
	}
	atom.view.RUnlock()
	return
}

func (atom *Atom[B, C, V]) Swap(swap func(block B, val V) (entry []byte, newVal V, err error)) (err error) {
	atom.mutex.Lock()
	defer atom.mutex.Unlock()

	oldCkpt := atom.ckpt
	var nilCkpt C
	if oldCkpt == nilCkpt {
		return ErrClosed
	}

	entry, newVal, err := swap(atom.block, atom.val)
	if err != nil {
		atom.block.Rollback()
		return
	}

	newCkpt, err := atom.block.Commit(entry)
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
