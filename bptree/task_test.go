package bptree

import (
	"errors"
	"strings"
	"testing"
)

func TestTaskRunSuccess(t *testing.T) {
	var task task
	executed := false

	task.run(func() error {
		executed = true
		return nil
	})

	err := task.wait()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if !executed {
		t.Error("task function was not executed")
	}
}

func TestTaskRunError(t *testing.T) {
	var task task
	expectedErr := errors.New("test error")

	task.run(func() error {
		return expectedErr
	})

	err := task.wait()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestTaskRunPanic(t *testing.T) {
	var task task

	task.run(func() error {
		panic("test panic")
	})

	err := task.wait()
	if err == nil {
		t.Fatal("expected error from panic, got nil")
	}
	if !strings.Contains(err.Error(), "recovered💊") {
		t.Errorf("expected panic error message, got: %v", err)
	}
}

func TestTaskRunPanicError(t *testing.T) {
	var task task
	panicErr := errors.New("panic error")

	task.run(func() error {
		panic(panicErr)
	})

	err := task.wait()
	if err == nil {
		t.Fatal("expected error from panic, got nil")
	}
	if !errors.Is(err, panicErr) {
		t.Errorf("expected panic error %v, got %v", panicErr, err)
	}
}

func TestTaskMultipleRuns(t *testing.T) {
	var task task
	err1 := errors.New("error 1")
	err2 := errors.New("error 2")

	task.run(func() error {
		return err1
	})
	task.run(func() error {
		return err2
	})

	err := task.wait()
	if err == nil {
		t.Fatal("expected errors, got nil")
	}

	taskErr, ok := err.(*taskerr)
	if !ok {
		t.Fatal("expected *taskerr type")
	}

	errs := taskErr.Unwrap()
	if len(errs) != 2 {
		t.Errorf("expected 2 errors, got %d", len(errs))
	}
}

func TestTaskWaitNoError(t *testing.T) {
	var task task

	task.run(func() error {
		return nil
	})
	task.run(func() error {
		return nil
	})

	err := task.wait()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestTaskerrError(t *testing.T) {
	err1 := errors.New("first error")
	err2 := errors.New("second error")

	taskErr := &taskerr{
		err: err1,
		next: &taskerr{
			err: err2,
		},
	}

	msg := taskErr.Error()
	if !strings.Contains(msg, "first error") {
		t.Errorf("expected 'first error' in message, got: %s", msg)
	}
	if !strings.Contains(msg, "second error") {
		t.Errorf("expected 'second error' in message, got: %s", msg)
	}
}

func TestAnyvError(t *testing.T) {
	av := anyv{any: "test value"}
	msg := av.Error()
	if !strings.Contains(msg, "recovered💊") {
		t.Errorf("expected 'recovered💊' in message, got: %s", msg)
	}
	if !strings.Contains(msg, "test value") {
		t.Errorf("expected 'test value' in message, got: %s", msg)
	}
}
