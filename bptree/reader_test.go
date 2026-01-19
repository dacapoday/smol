// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package bptree

import (
	"bytes"
	"testing"

	"github.com/dacapoday/smol/block"
	"github.com/dacapoday/smol/mem"
)

func TestReaderBasic(t *testing.T) {
	var f mem.File
	file := &f

	var b block.Heap[*mem.File]
	blk := &b

	_, ckpt, err := blk.Load(file, option{})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	defer ckpt.Release()
	defer blk.Close()

	// Create test data using mock leaf items
	itemCount := 15
	keyLen := 16
	valLen := 36
	leafItems := newMockLeafItems(itemCount, keyLen, valLen)

	// Write root using writeRoot
	high, rootPage, err := writeRoot(blk, makePage, 0, leafItems)
	if err != nil {
		t.Fatalf("writeRoot failed: %v", err)
	}

	root := &Root{
		high: high,
		page: rootPage,
		klen: 1000, // Large enough to avoid overflow
		vlen: 1000, // Large enough to avoid overflow
	}

	// Create reader and load
	var reader Reader[*block.Heap[*mem.File], *Root]
	reader.Load(blk, root)
	defer reader.Close()

	// Use SeekFirst to start iteration
	if !reader.SeekFirst() {
		t.Fatal("SeekFirst failed")
	}

	for k, v := range leafItems {
		key := reader.Key()
		val := reader.Val()
		if !bytes.Equal(key, k) {
			t.Fatalf("mismatch key got %v, want %v", string(key), string(k))
		}
		if !bytes.Equal(val, v) {
			t.Fatalf("mismatch val got %v, want %v", string(key), string(k))
		}
		reader.Next()
	}

	if reader.Next() {
		t.Fatal("Should no more items")
	}

	// Check error
	if err := reader.Error(); err != nil {
		t.Fatalf("Reader error: %v", err)
	}
}

func TestReaderEmptyTree(t *testing.T) {
	// Setup block storage
	var f mem.File
	file := &f

	var b block.Heap[*mem.File]
	blk := &b

	_, ckpt, err := blk.Load(file, option{})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	defer ckpt.Release()
	defer blk.Close()

	// Create empty root
	root := &Root{
		high: 0,
		page: nil,
		klen: 1000, // Large enough to avoid overflow
		vlen: 1000, // Large enough to avoid overflow
	}

	// Create reader and load
	var reader Reader[*block.Heap[*mem.File], *Root]
	reader.Load(blk, root)
	defer reader.Close()

	// Try to seek first - should fail on empty tree
	if reader.SeekFirst() {
		t.Fatal("SeekFirst should fail on empty tree")
	}

	if reader.SeekLast() {
		t.Fatal("SeekLast should fail on empty tree")
	}

	if reader.Valid() {
		t.Fatal("Reader should not be valid on empty tree")
	}
}
