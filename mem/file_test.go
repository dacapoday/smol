package mem

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

// TestFileReadWrite tests basic read and write operations
func TestFileReadWrite(t *testing.T) {
	var f File
	defer f.Close()

	// Write data at different offsets
	data1 := []byte("hello")
	n, err := f.WriteAt(data1, 0)
	if err != nil || n != len(data1) {
		t.Fatalf("WriteAt failed: n=%d, err=%v", n, err)
	}

	data2 := []byte("world")
	n, err = f.WriteAt(data2, 10)
	if err != nil || n != len(data2) {
		t.Fatalf("WriteAt failed: n=%d, err=%v", n, err)
	}

	// Read back and verify
	buf := make([]byte, 5)
	n, err = f.ReadAt(buf, 0)
	if err != nil || n != 5 || string(buf) != "hello" {
		t.Errorf("ReadAt(0): got %q, want %q", buf, "hello")
	}

	n, err = f.ReadAt(buf, 10)
	if err != nil || n != 5 || string(buf) != "world" {
		t.Errorf("ReadAt(10): got %q, want %q", buf, "world")
	}

	// Verify middle gap is zeros
	gap := make([]byte, 5)
	n, err = f.ReadAt(gap, 5)
	if err != nil || n != 5 {
		t.Fatalf("ReadAt gap failed: n=%d, err=%v", n, err)
	}
	for i, b := range gap {
		if b != 0 {
			t.Errorf("gap[%d] = %d, want 0", i, b)
		}
	}
}

// TestFileExpansion tests automatic file expansion when writing beyond current size
func TestFileExpansion(t *testing.T) {
	var f File
	defer f.Close()

	// File starts empty
	if size := f.Size(); size != 0 {
		t.Errorf("initial size = %d, want 0", size)
	}

	// Write at offset 100 should expand file to 105
	data := []byte("hello")
	n, err := f.WriteAt(data, 100)
	if err != nil || n != 5 {
		t.Fatalf("WriteAt failed: n=%d, err=%v", n, err)
	}

	if size := f.Size(); size != 105 {
		t.Errorf("size after write = %d, want 105", size)
	}

	// Verify data is readable
	buf := make([]byte, 5)
	n, err = f.ReadAt(buf, 100)
	if err != nil || string(buf) != "hello" {
		t.Errorf("ReadAt: got %q, want %q", buf, "hello")
	}

	// Verify gaps are zero-filled
	zeros := make([]byte, 100)
	n, err = f.ReadAt(zeros, 0)
	if err != nil || n != 100 {
		t.Fatalf("ReadAt zeros failed: n=%d, err=%v", n, err)
	}
	for i, b := range zeros {
		if b != 0 {
			t.Errorf("zeros[%d] = %d, want 0", i, b)
		}
	}
}

// TestFileReadFromWriteTo tests ReadFrom and WriteTo operations
func TestFileReadFromWriteTo(t *testing.T) {
	var f File
	defer f.Close()

	// ReadFrom replaces entire file content
	input := "the quick brown fox jumps over the lazy dog"
	n, err := f.ReadFrom(strings.NewReader(input))
	if err != nil || n != int64(len(input)) {
		t.Fatalf("ReadFrom failed: n=%d, err=%v", n, err)
	}

	if size := f.Size(); size != int64(len(input)) {
		t.Errorf("size = %d, want %d", size, len(input))
	}

	// WriteTo should write entire content
	var buf bytes.Buffer
	n, err = f.WriteTo(&buf)
	if err != nil || n != int64(len(input)) {
		t.Fatalf("WriteTo failed: n=%d, err=%v", n, err)
	}

	if buf.String() != input {
		t.Errorf("WriteTo content = %q, want %q", buf.String(), input)
	}

	// ReadFrom again should replace content
	newInput := "new content"
	n, err = f.ReadFrom(strings.NewReader(newInput))
	if err != nil || n != int64(len(newInput)) {
		t.Fatalf("ReadFrom #2 failed: n=%d, err=%v", n, err)
	}

	if size := f.Size(); size != int64(len(newInput)) {
		t.Errorf("size after second ReadFrom = %d, want %d", size, len(newInput))
	}
}

// TestFileTruncate tests file truncation and expansion
func TestFileTruncate(t *testing.T) {
	var f File
	defer f.Close()

	// Write initial data
	data := []byte("hello world")
	f.WriteAt(data, 0)

	// Truncate to smaller size
	err := f.Truncate(5)
	if err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	if size := f.Size(); size != 5 {
		t.Errorf("size after truncate = %d, want 5", size)
	}

	// Verify only first 5 bytes remain
	buf := make([]byte, 5)
	n, err := f.ReadAt(buf, 0)
	if err != nil || string(buf) != "hello" {
		t.Errorf("ReadAt after truncate: got %q, want %q", buf, "hello")
	}

	// Reading beyond truncated size should return EOF
	_, err = f.ReadAt(buf, 5)
	if err != io.EOF {
		t.Errorf("ReadAt beyond truncate: err = %v, want EOF", err)
	}

	// Truncate to larger size (expansion with zeros)
	err = f.Truncate(10)
	if err != nil {
		t.Fatalf("Truncate expand failed: %v", err)
	}

	if size := f.Size(); size != 10 {
		t.Errorf("size after expand = %d, want 10", size)
	}

	// Verify expansion is zero-filled
	buf = make([]byte, 10)
	n, err = f.ReadAt(buf, 0)
	if err != nil || n != 10 {
		t.Fatalf("ReadAt after expand failed: n=%d, err=%v", n, err)
	}

	expected := []byte("hello\x00\x00\x00\x00\x00")
	if !bytes.Equal(buf, expected) {
		t.Errorf("content after expand = %q, want %q", buf, expected)
	}
}

// TestFileCloseClears tests that Close clears all data and resets the file.
// Writes 3 bytes, closes the file, then verifies:
// - Size is 0
// - Reading returns io.EOF
func TestFileCloseClears(t *testing.T) {
	var f File
	data := []byte("abc")
	if _, err := f.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}
	if got := f.Size(); got != 0 {
		t.Fatalf("Size after Close = %d, want 0", got)
	}
	buf := make([]byte, 1)
	if n, err := f.ReadAt(buf, 0); err != io.EOF && n != 0 {
		t.Fatalf("ReadAt after Close unexpected n=%d err=%v", n, err)
	}
}

// TestFileEdgeCases tests edge cases and error conditions
func TestFileEdgeCases(t *testing.T) {
	var f File
	defer f.Close()

	// Negative offset should return error
	_, err := f.WriteAt([]byte("test"), -1)
	if err != io.ErrUnexpectedEOF {
		t.Errorf("WriteAt negative offset: err = %v, want ErrUnexpectedEOF", err)
	}

	_, err = f.ReadAt([]byte{0}, -1)
	if err != io.ErrUnexpectedEOF {
		t.Errorf("ReadAt negative offset: err = %v, want ErrUnexpectedEOF", err)
	}

	// Empty write/read should succeed
	n, err := f.WriteAt([]byte{}, 0)
	if err != nil || n != 0 {
		t.Errorf("WriteAt empty: n=%d, err=%v", n, err)
	}

	n, err = f.ReadAt([]byte{}, 0)
	if err != nil || n != 0 {
		t.Errorf("ReadAt empty: n=%d, err=%v", n, err)
	}

	// Read from empty file should return EOF
	_, err = f.ReadAt([]byte{0}, 0)
	if err != io.EOF {
		t.Errorf("ReadAt empty file: err = %v, want EOF", err)
	}

	// Sync should always succeed (no-op)
	if err := f.Sync(); err != nil {
		t.Errorf("Sync failed: %v", err)
	}
}
