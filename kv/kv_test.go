package kv

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/dacapoday/smol/bptree"
	"github.com/dacapoday/smol/mem"
)

// TestKVSetGet tests basic Set and Get operations.
// Sets a single key-value pair and reads it back.
func TestKVSetGet(t *testing.T) {
	var file mem.File
	var kv KV[*mem.File]

	err := kv.Load(&file)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer kv.Close()

	key := []byte("hello")
	val := []byte("world")

	err = kv.Set(key, val)
	if err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := kv.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !bytes.Equal(got, val) {
		t.Fatalf("Get = %q, want %q", got, val)
	}

	t.Logf("✓ Set and Get: key=%q val=%q", key, val)
}

// TestKVSetGetMultiple tests multiple key-value pairs.
// Sets 100 keys with sequential values and reads them back.
func TestKVSetGetMultiple(t *testing.T) {
	var file mem.File
	var kv KV[*mem.File]

	err := kv.Load(&file)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer kv.Close()

	count := 100
	for i := range count {
		key := fmt.Appendf(nil, "key-%03d", i)
		val := fmt.Appendf(nil, "value-%03d", i)

		err = kv.Set(key, val)
		if err != nil {
			t.Fatalf("Set[%d]: %v", i, err)
		}
	}

	for i := range count {
		key := fmt.Appendf(nil, "key-%03d", i)
		expected := fmt.Appendf(nil, "value-%03d", i)

		got, err := kv.Get(key)
		if err != nil {
			t.Fatalf("Get[%d]: %v", i, err)
		}

		if !bytes.Equal(got, expected) {
			t.Fatalf("Get[%d] = %q, want %q", i, got, expected)
		}
	}

	t.Logf("✓ Set and Get %d key-value pairs", count)
}

// TestKVGetNonExistent tests getting a non-existent key.
// Verifies that Get returns nil for keys that don't exist.
func TestKVGetNonExistent(t *testing.T) {
	var file mem.File
	var kv KV[*mem.File]

	err := kv.Load(&file)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer kv.Close()

	key := []byte("nonexistent")
	got, err := kv.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got != nil {
		t.Fatalf("Get non-existent key = %q, want nil", got)
	}

	t.Log("✓ Get non-existent key returns nil")
}

// TestKVOverwrite tests overwriting an existing key.
// Sets a key twice with different values and verifies the latest value is returned.
func TestKVOverwrite(t *testing.T) {
	var file mem.File
	var kv KV[*mem.File]

	err := kv.Load(&file)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer kv.Close()

	key := []byte("key")
	val1 := []byte("value1")
	val2 := []byte("value2")

	err = kv.Set(key, val1)
	if err != nil {
		t.Fatalf("Set first: %v", err)
	}

	err = kv.Set(key, val2)
	if err != nil {
		t.Fatalf("Set second: %v", err)
	}

	got, err := kv.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !bytes.Equal(got, val2) {
		t.Fatalf("Get after overwrite = %q, want %q", got, val2)
	}

	t.Logf("✓ Overwrite: key=%q old=%q new=%q", key, val1, val2)
}

// TestKVBatch tests batch write operations.
// Writes 50 key-value pairs in a single batch and reads them back.
func TestKVBatch(t *testing.T) {
	var file mem.File
	var kv KV[*mem.File]

	err := kv.Load(&file)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer kv.Close()

	count := 50
	err = kv.Batch(func(yield func([]byte, []byte) bool) {
		for i := range count {
			key := fmt.Appendf(nil, "batch-%03d", i)
			val := fmt.Appendf(nil, "data-%03d", i)
			if !yield(key, val) {
				break
			}
		}
	})
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}

	for i := range count {
		key := fmt.Appendf(nil, "batch-%03d", i)
		expected := fmt.Appendf(nil, "data-%03d", i)

		got, err := kv.Get(key)
		if err != nil {
			t.Fatalf("Get[%d]: %v", i, err)
		}

		if !bytes.Equal(got, expected) {
			t.Fatalf("Get[%d] = %q, want %q", i, got, expected)
		}
	}

	t.Logf("✓ Batch write %d key-value pairs", count)
}

// TestKVClose tests that operations fail after Close.
// Closes the KV and verifies that Get returns ErrClosed.
func TestKVClose(t *testing.T) {
	var file mem.File
	var kv KV[*mem.File]

	err := kv.Load(&file)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	err = kv.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}

	key := []byte("key")
	val := []byte("value")

	err = kv.Set(key, val)
	if !errors.Is(err, bptree.ErrClosed) {
		t.Fatalf("Set after Close: err=%v, want ErrClosed", err)
	}

	_, err = kv.Get(key)
	if !errors.Is(err, bptree.ErrClosed) {
		t.Fatalf("Get after Close: err=%v, want ErrClosed", err)
	}

	t.Log("✓ Get after Close returns ErrClosed")
}

// TestKVLoadReopen tests loading data after reopening.
// Writes data, saves to buffer, then loads from buffer and verifies data persists.
func TestKVLoadReopen(t *testing.T) {
	var file1 mem.File

	// First session: write data
	var kv1 KV[*mem.File]
	err := kv1.Load(&file1)
	if err != nil {
		t.Fatalf("Load first: %v", err)
	}

	count := 20
	for i := range count {
		key := fmt.Appendf(nil, "key-%02d", i)
		val := fmt.Appendf(nil, "val-%02d", i)
		err = kv1.Set(key, val)
		if err != nil {
			t.Fatalf("Set[%d]: %v", i, err)
		}
	}

	// Save to buffer before closing
	var buf bytes.Buffer
	_, err = file1.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}

	err = kv1.Close()
	if err != nil {
		t.Fatalf("Close first: %v", err)
	}

	// Second session: load from buffer and verify
	var file2 mem.File
	_, err = file2.ReadFrom(&buf)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}

	var kv2 KV[*mem.File]
	err = kv2.Load(&file2)
	if err != nil {
		t.Fatalf("Load second: %v", err)
	}
	defer kv2.Close()

	for i := range count {
		key := fmt.Appendf(nil, "key-%02d", i)
		expected := fmt.Appendf(nil, "val-%02d", i)

		got, err := kv2.Get(key)
		if err != nil {
			t.Fatalf("Get[%d] after reopen: %v", i, err)
		}

		if !bytes.Equal(got, expected) {
			t.Fatalf("Get[%d] after reopen = %q, want %q", i, got, expected)
		}
	}

	t.Logf("✓ Saved, loaded and verified %d key-value pairs", count)
}

// TestKVLargeValue tests storing and retrieving large values.
// Tests with values of 1KB, 4KB, 16KB, and 64KB.
func TestKVLargeValue(t *testing.T) {
	var file mem.File
	var kv KV[*mem.File]

	err := kv.Load(&file)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer kv.Close()

	sizes := []int{1024, 4096, 16384, 65536}

	for _, size := range sizes {
		key := fmt.Appendf(nil, "large-%d", size)
		val := make([]byte, size)
		for i := range val {
			val[i] = byte(i % 256)
		}

		err = kv.Set(key, val)
		if err != nil {
			t.Fatalf("Set size=%d: %v", size, err)
		}

		got, err := kv.Get(key)
		if err != nil {
			t.Fatalf("Get size=%d: %v", size, err)
		}

		if !bytes.Equal(got, val) {
			t.Fatalf("Get size=%d: data mismatch", size)
		}

		t.Logf("✓ Large value: size=%d (%.1fKB)", size, float64(size)/1024)
	}
}

// TestKVEmptyValue tests storing empty values.
// Sets a key with an empty byte slice and retrieves it.
func TestKVEmptyValue(t *testing.T) {
	var file mem.File
	var kv KV[*mem.File]

	err := kv.Load(&file)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer kv.Close()

	key := []byte("empty")
	val := []byte{}

	err = kv.Set(key, val)
	if err != nil {
		t.Fatalf("Set empty: %v", err)
	}

	got, err := kv.Get(key)
	if err != nil {
		t.Fatalf("Get empty: %v", err)
	}
	if got == nil {
		t.Fatalf("Get empty value = nil, want empty slice")
	}

	if !bytes.Equal(got, val) {
		t.Fatalf("Get empty value = %q, want %q", got, val)
	}

	t.Log("✓ Empty value stored and retrieved")
}

// TestKVBatchOverwrite tests batch operations that overwrite existing keys.
// Sets initial values, then overwrites them in a batch.
func TestKVBatchOverwrite(t *testing.T) {
	var file mem.File
	var kv KV[*mem.File]

	err := kv.Load(&file)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer kv.Close()

	count := 30
	for i := range count {
		key := fmt.Appendf(nil, "key-%02d", i)
		val := fmt.Appendf(nil, "old-%02d", i)
		err = kv.Set(key, val)
		if err != nil {
			t.Fatalf("Set initial[%d]: %v", i, err)
		}
	}

	err = kv.Batch(func(yield func([]byte, []byte) bool) {
		for i := range count {
			key := fmt.Appendf(nil, "key-%02d", i)
			val := fmt.Appendf(nil, "new-%02d", i)
			if !yield(key, val) {
				break
			}
		}
	})
	if err != nil {
		t.Fatalf("Batch overwrite: %v", err)
	}

	for i := range count {
		key := fmt.Appendf(nil, "key-%02d", i)
		expected := fmt.Appendf(nil, "new-%02d", i)

		got, err := kv.Get(key)
		if err != nil {
			t.Fatalf("Get[%d]: %v", i, err)
		}

		if !bytes.Equal(got, expected) {
			t.Fatalf("Get[%d] = %q, want %q", i, got, expected)
		}
	}

	t.Logf("✓ Batch overwrite %d keys", count)
}
