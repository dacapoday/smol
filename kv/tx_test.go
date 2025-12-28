package kv

import (
	"bytes"
	"testing"

	"github.com/dacapoday/smol/mem"
)

// TestTxReadCommitted tests Read Committed isolation level.
// Verifies that uncommitted changes are visible within the transaction
// but not outside until commit.
func TestTxReadCommitted(t *testing.T) {
	var file mem.File
	var kv KV[*mem.File]

	err := kv.Load(&file)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer kv.Close()

	// Initialize data
	kv.Set([]byte("a"), []byte("1"))
	kv.Set([]byte("b"), []byte("2"))
	kv.Set([]byte("c"), []byte("3"))

	// Begin transaction
	tx := kv.Begin()

	// Test 1: tx.Get() sees uncommitted changes
	tx.Set([]byte("b"), []byte("modified"))
	tx.Set([]byte("d"), []byte("new"))

	// Inside transaction: see uncommitted changes
	valB, _ := tx.Get([]byte("b"))
	if !bytes.Equal(valB, []byte("modified")) {
		t.Errorf("tx.Get(b) = %q, want %q", valB, "modified")
	}

	valD, _ := tx.Get([]byte("d"))
	if !bytes.Equal(valD, []byte("new")) {
		t.Errorf("tx.Get(d) = %q, want %q", valD, "new")
	}

	// Outside transaction: don't see uncommitted changes
	valBOuter, _ := kv.Get([]byte("b"))
	if !bytes.Equal(valBOuter, []byte("2")) {
		t.Errorf("kv.Get(b) = %q, want %q (should not see uncommitted)", valBOuter, "2")
	}

	valDOuter, _ := kv.Get([]byte("d"))
	if valDOuter != nil {
		t.Errorf("kv.Get(d) = %q, want nil (should not see uncommitted)", valDOuter)
	}

	// Test 2: tx.Iter() sees uncommitted changes
	iter := tx.Iter()
	defer iter.Close()

	expected := map[string]string{
		"a": "1",
		"b": "modified", // uncommitted change
		"c": "3",
		"d": "new", // uncommitted addition
	}

	count := 0
	for iter.SeekFirst(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		val := string(iter.Val())

		expectedVal, ok := expected[key]
		if !ok {
			t.Errorf("Unexpected key in tx.Iter: %q", key)
			continue
		}

		if val != expectedVal {
			t.Errorf("tx.Iter[%s] = %q, want %q", key, val, expectedVal)
		}

		count++
	}

	if count != len(expected) {
		t.Errorf("tx.Iter count = %d, want %d", count, len(expected))
	}

	// Rollback discards all changes
	tx.Rollback()

	// After rollback: original values
	valB, _ = kv.Get([]byte("b"))
	if !bytes.Equal(valB, []byte("2")) {
		t.Errorf("After rollback: kv.Get(b) = %q, want %q", valB, "2")
	}

	valD, _ = kv.Get([]byte("d"))
	if valD != nil {
		t.Errorf("After rollback: kv.Get(d) = %q, want nil", valD)
	}

	t.Log("✓ Read Committed isolation verified")
}

// TestTxCommit tests that committed changes become visible.
func TestTxCommit(t *testing.T) {
	var file mem.File
	var kv KV[*mem.File]

	err := kv.Load(&file)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer kv.Close()

	// Initialize
	kv.Set([]byte("x"), []byte("old"))

	// Transaction: modify and commit
	tx := kv.Begin()
	tx.Set([]byte("x"), []byte("new"))
	tx.Set([]byte("y"), []byte("added"))

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// After commit: changes are visible
	valX, _ := kv.Get([]byte("x"))
	if !bytes.Equal(valX, []byte("new")) {
		t.Errorf("After commit: kv.Get(x) = %q, want %q", valX, "new")
	}

	valY, _ := kv.Get([]byte("y"))
	if !bytes.Equal(valY, []byte("added")) {
		t.Errorf("After commit: kv.Get(y) = %q, want %q", valY, "added")
	}

	t.Log("✓ Commit makes changes visible")
}
