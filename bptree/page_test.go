package bptree

import (
	"bytes"
	"math"
	"math/rand/v2"
	"testing"
)

func TestLeafPageRoundTrip(t *testing.T) {
	pageSize := 512 + rand.IntN(64*1024-511)
	maxKeyOverflow := math.MaxInt64
	maxValOverflow := math.MaxInt64
	branchFactor := 4

	keyInlineSize, valInlineSize := InlineSize(pageSize, branchFactor, maxKeyOverflow, maxValOverflow)
	t.Logf("pageSize=%d, keyInline=%d, valInline=%d", pageSize, keyInlineSize, valInlineSize)

	var allKeys [][]byte
	var allVals [][]byte

	for range 1000 {
		keyLen := 1 + rand.IntN(keyInlineSize)
		valLen := 1 + rand.IntN(valInlineSize)
		key := make([]byte, keyLen)
		val := make([]byte, valLen)
		for j := range key {
			key[j] = byte(rand.IntN(256))
		}
		for j := range val {
			val[j] = byte(rand.IntN(256))
		}
		allKeys = append(allKeys, key)
		allVals = append(allVals, val)
	}

	usedSize := HeadSize
	itemCount := 0
	for i := 0; i < len(allKeys); i++ {
		itemSize := leafItemSize(len(allKeys[i]), len(allVals[i]))
		if usedSize+itemSize > pageSize {
			break
		}
		usedSize += itemSize
		itemCount++
	}

	t.Logf("itemCount=%d, usedSize=%d", itemCount, usedSize)

	originalKeys := allKeys[:itemCount]
	originalVals := allVals[:itemCount]

	items := LeafItems(func(yield func([]byte, []byte) bool) {
		for i := 0; i < itemCount; i++ {
			if !yield(originalKeys[i], originalVals[i]) {
				return
			}
		}
	})

	buffer := make([]byte, pageSize)
	encodeLeafPage(buffer, items)
	page := Page(buffer)

	if !page.IsLeaf() {
		t.Fatal("page should be a leaf page")
	}

	if page.Count() != uint16(itemCount) {
		t.Fatalf("expected %d items, got %d", itemCount, page.Count())
	}

	for i := uint16(0); i < uint16(itemCount); i++ {
		key := page.LeafKey(i)
		val := page.LeafVal(i)

		if !bytes.Equal(key, originalKeys[i]) {
			t.Errorf("item %d: key mismatch\nexpected: %x\ngot:      %x", i, originalKeys[i], key)
		}

		if !bytes.Equal(val, originalVals[i]) {
			t.Errorf("item %d: val mismatch\nexpected: %x\ngot:      %x", i, originalVals[i], val)
		}
	}
}

func TestBranchPageRoundTrip(t *testing.T) {
	pageSize := 512 + rand.IntN(64*1024-511)
	maxKeyOverflow := math.MaxInt64
	maxValOverflow := math.MaxInt64
	branchFactor := 4

	keyInlineSize, _ := InlineSize(pageSize, branchFactor, maxKeyOverflow, maxValOverflow)
	t.Logf("pageSize=%d, keyInline=%d", pageSize, keyInlineSize)

	var allKeys [][]byte
	var allIDs []BlockID

	for range 1000 {
		keyLen := 1 + rand.IntN(keyInlineSize)
		key := make([]byte, keyLen)
		for j := range key {
			key[j] = byte(rand.IntN(256))
		}
		allKeys = append(allKeys, key)
		allIDs = append(allIDs, 2+rand.Uint32())
	}

	usedSize := HeadSize
	itemCount := 0
	for i := 0; i < len(allKeys); i++ {
		itemSize := branchItemSize(len(allKeys[i]))
		if usedSize+itemSize > pageSize {
			break
		}
		usedSize += itemSize
		itemCount++
	}

	t.Logf("itemCount=%d, usedSize=%d", itemCount, usedSize)

	originalKeys := allKeys[:itemCount]
	originalIDs := allIDs[:itemCount]

	items := BranchItems(func(yield func([]byte, BlockID) bool) {
		for i := 0; i < itemCount; i++ {
			if !yield(originalKeys[i], originalIDs[i]) {
				return
			}
		}
	})

	buffer := make([]byte, pageSize)
	encodeBranchPage(buffer, items)
	page := Page(buffer)

	if page.IsLeaf() {
		t.Fatal("page should be a branch page")
	}

	if page.Count() != uint16(itemCount) {
		t.Fatalf("expected %d items, got %d", itemCount, page.Count())
	}

	for i := uint16(0); i < uint16(itemCount); i++ {
		key := page.BranchKey(i)
		id := page.BranchID(i)

		if !bytes.Equal(key, originalKeys[i]) {
			t.Errorf("item %d: key mismatch\nexpected: %x\ngot:      %x", i, originalKeys[i], key)
		}

		if id != originalIDs[i] {
			t.Errorf("item %d: blockID mismatch, expected %d, got %d", i, originalIDs[i], id)
		}
	}
}

func TestEmptyPage(t *testing.T) {
	var page Page

	if page.Count() != 0 {
		t.Errorf("empty page Count() = %d, want 0", page.Count())
	}
	if !page.IsLeaf() {
		t.Error("empty page IsLeaf() = false, want true")
	}
	if page.Size() != 0 {
		t.Errorf("empty page Size() = %d, want 0", page.Size())
	}

	shortPage := make(Page, 2)
	if shortPage.Count() != 0 {
		t.Errorf("short page Count() = %d, want 0", shortPage.Count())
	}
	if !shortPage.IsLeaf() {
		t.Error("short page IsLeaf() = false, want true")
	}
}
