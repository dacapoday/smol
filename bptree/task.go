// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package bptree

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type task struct {
	sync.WaitGroup
	head atomic.Pointer[taskerr]
}

func (task *task) run(f func() error) {
	task.Add(1)
	go func() {
		end := false
		defer func() {
			task.Done()
			if end {
				return
			}
			switch v := recover().(type) {
			case nil:
			case error:
				task.push(v)
			default:
				task.push(anyv{v})
			}
		}()
		if err := f(); err != nil {
			task.push(err)
		}
		end = true
	}()
}

func (task *task) push(err error) {
	e := &taskerr{err: err}
	for {
		head := task.head.Load()
		e.next = head
		if task.head.CompareAndSwap(head, e) {
			return
		}
	}
}

func (task *task) wait() error {
	task.Wait()
	head := task.head.Swap(nil)
	if head == nil {
		return nil
	}
	return head
}

type taskerr struct {
	next *taskerr
	err  error
}

func (task *taskerr) each(yield func(error) bool) {
	for ; task != nil; task = task.next {
		if !yield(task.err) {
			return
		}
	}
}

func (task *taskerr) Error() string {
	var msg []byte
	for err := range task.each {
		msg = append(msg, '\n')
		msg = append(msg, err.Error()...)
	}
	if len(msg) == 0 {
		return ""
	}
	return b2s(msg[1:])
}

func (task *taskerr) Unwrap() (errs []error) {
	for err := range task.each {
		errs = append(errs, err)
	}
	return
}

type anyv struct{ any }

func (v anyv) Error() string {
	return fmt.Sprintf("recovered💊: %v", v.any)
}
