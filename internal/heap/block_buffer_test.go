// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package heap

import (
	"bytes"
	"testing"
)

func TestBuffer_Write(t *testing.T) {
	// Test successful write within capacity
	t.Run("successful write", func(t *testing.T) {
		// Create a buffer with capacity 10
		buf := make(Buffer, 0, 10)

		data := []byte("hello")
		n, err := buf.Write(data)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if n != len(data) {
			t.Errorf("expected %d bytes written, got %d", len(data), n)
		}
		if !bytes.Equal(buf, data) {
			t.Errorf("expected buffer to contain 'hello', got '%s'", string(buf))
		}
		if len(buf) != 5 {
			t.Errorf("expected buffer length 5, got %d", len(buf))
		}
	})

	// Test multiple writes within capacity
	t.Run("multiple writes", func(t *testing.T) {
		buf := make(Buffer, 0, 10)

		// First write
		n1, err1 := buf.Write([]byte("abc"))
		if err1 != nil || n1 != 3 {
			t.Errorf("first write failed: n=%d, err=%v", n1, err1)
		}

		// Second write
		n2, err2 := buf.Write([]byte("def"))
		if err2 != nil || n2 != 3 {
			t.Errorf("second write failed: n=%d, err=%v", n2, err2)
		}

		if !bytes.Equal(buf, []byte("abcdef")) {
			t.Errorf("expected buffer to contain 'abcdef', got '%s'", string(buf))
		}
	})

	// Test write exceeding capacity
	t.Run("write exceeding capacity", func(t *testing.T) {
		buf := make(Buffer, 0, 5)

		// Fill the buffer to capacity
		data := []byte("hello")
		buf.Write(data)

		// Try to write more data
		n, err := buf.Write([]byte("world"))

		if err != ErrOutOfRange {
			t.Errorf("expected ErrOutOfRange, got %v", err)
		}
		if n != 0 {
			t.Errorf("expected 0 bytes written, got %d", n)
		}
		if !bytes.Equal(buf, data) {
			t.Errorf("buffer should remain unchanged, got '%s'", string(buf))
		}
	})

	// Test single write exceeding capacity
	t.Run("single write exceeding capacity", func(t *testing.T) {
		buf := make(Buffer, 0, 3)

		data := []byte("hello")
		n, err := buf.Write(data)

		if err != ErrOutOfRange {
			t.Errorf("expected ErrOutOfRange, got %v", err)
		}
		if n != 0 {
			t.Errorf("expected 0 bytes written, got %d", n)
		}
		if len(buf) != 0 {
			t.Errorf("buffer should remain empty, length=%d", len(buf))
		}
	})

	// Test write to exact capacity
	t.Run("write to exact capacity", func(t *testing.T) {
		buf := make(Buffer, 0, 5)

		data := []byte("hello")
		n, err := buf.Write(data)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if n != 5 {
			t.Errorf("expected 5 bytes written, got %d", n)
		}
		if !bytes.Equal(buf, data) {
			t.Errorf("expected buffer to contain 'hello', got '%s'", string(buf))
		}
		if len(buf) != cap(buf) {
			t.Errorf("buffer length should equal capacity: len=%d, cap=%d", len(buf), cap(buf))
		}
	})
}
