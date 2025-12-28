// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package bptree

import (
	"testing"

	"github.com/dacapoday/smol/block"
	"github.com/dacapoday/smol/mem"
)

func TestWriteRootPageSinglePage(t *testing.T) {
	// Setup block storage
	var f mem.File
	var b block.CRC32Heap[*mem.File]

	_, ckpt, err := b.Load(&f, option{})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	defer ckpt.Release()
	defer b.Close()

	// Small dataset that fits in one page
	items := newMockLeafItems(5, 8, 16)

	// Call writeRootPage
	root, branch, err := writeRootPage(&b, items)
	if err != nil {
		t.Fatalf("writeRootPage failed: %v", err)
	}

	// Should return root page, no branch
	if root == nil {
		t.Error("Expected root page, got nil")
	}
	if branch != nil {
		t.Error("Expected no branch items for single page")
	}

	// Verify root page has correct item count
	if root != nil {
		count := root.Count()
		if count != 5 {
			t.Errorf("Root page count: got %d, want 5", count)
		}
		if !root.IsLeaf() {
			t.Error("Root page should be a leaf page")
		}
	}
}

func TestWriteRootPageMultiplePages(t *testing.T) {
	// Setup block storage
	var f mem.File
	var b block.CRC32Heap[*mem.File]

	_, ckpt, err := b.Load(&f, option{})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	defer ckpt.Release()
	defer b.Close()

	// Large dataset that needs multiple pages
	items := newMockLeafItems(20, 16, 36) // assume page size is 512

	// Call writeRootPage
	root, branch, err := writeRootPage(&b, items)
	if err != nil {
		t.Fatalf("writeRootPage failed: %v", err)
	}

	// Should return branch items, no root
	if root != nil {
		t.Error("Expected no root page for multi-page dataset")
	}
	if branch == nil {
		t.Fatal("Expected branch items for multi-page dataset")
	}

	// Count branch items
	branchCount := 0
	for range branch {
		branchCount++
	}

	if branchCount < 2 {
		t.Errorf("Expected at least 2 branch items, got %d", branchCount)
	}
}

func TestWriteRootPageEmpty(t *testing.T) {
	// Setup block storage
	var f mem.File
	var b block.CRC32Heap[*mem.File]

	_, ckpt, err := b.Load(&f, option{})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	defer ckpt.Release()
	defer b.Close()

	// Empty items
	items := newMockLeafItems(0, 8, 16)

	// Call writeRootPage
	root, branch, err := writeRootPage(&b, items)
	if err != nil {
		t.Fatalf("writeRootPage failed: %v", err)
	}

	// Both should be nil for empty items
	if root != nil {
		t.Error("Expected nil root for empty items")
	}
	if branch != nil {
		t.Error("Expected nil branch for empty items")
	}
}
