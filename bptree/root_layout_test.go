package bptree

import (
	"bytes"
	"testing"
)

func TestLayout_LeafItems_Empty(t *testing.T) {
	items := newMockLeafItems(0, 5, 10)
	next, stop := layout(items, 1024)
	defer stop()

	size, resultItems, last := next()

	// Empty items should return single empty result with HeadSize
	if !last {
		t.Errorf("Expected last=true for empty items")
	}
	if size != HeadSize {
		t.Errorf("Expected size=%d (HeadSize) for empty items, got: %d", HeadSize, size)
	}

	// Result items should be empty
	count := 0
	for range resultItems {
		count++
	}
	if count != 0 {
		t.Errorf("Expected 0 items in result, got: %d", count)
	}
}

func TestLayout_LeafItems_Various(t *testing.T) {
	testCases := []struct {
		name     string
		count    int
		keyLen   int
		valLen   int
		pageSize int
		desc     string
	}{
		{"single_item", 1, 5, 10, 1024, "single item in large page"},
		{"fits_one_page", 10, 3, 5, 1024, "multiple items fit in one page"},
		{"exact_one_page", 28, 5, 10, 512, "items exactly fill one page"}, // HeadSize(4) + 28*18 = 508, use 512 as minimum
		{"two_pages", 60, 5, 10, 512, "items require exactly two pages"},
		{"multiple_pages", 100, 3, 15, 512, "items require multiple pages"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Generate mock items and cache original kv pairs
			items := newMockLeafItems(tc.count, tc.keyLen, tc.valLen)
			var originalKVs []struct{ key, val []byte }
			for key, val := range items {
				originalKVs = append(originalKVs, struct{ key, val []byte }{key, val})
			}

			// Apply layout
			next, stop := layout(items, tc.pageSize)
			defer stop()

			// Collect all items from all pages in order
			var allResultKVs []struct{ key, val []byte }
			pageNum := 0

			for {
				size, resultItems, last := next()
				pageNum++

				// Verify page size doesn't exceed limit
				if size > tc.pageSize {
					t.Errorf("Page %d size=%d exceeds pageSize=%d", pageNum, size, tc.pageSize)
				}

				// Verify page size calculation
				expectedSize := HeadSize
				pageItemCount := 0
				for key, val := range resultItems {
					expectedSize += leafItemSize(len(key), len(val))
					pageItemCount++
					allResultKVs = append(allResultKVs, struct{ key, val []byte }{key, val})
				}

				if size != expectedSize {
					t.Errorf("Page %d: expected size=%d, got: %d", pageNum, expectedSize, size)
				}

				// Non-last pages should have at least one item
				if !last && pageItemCount == 0 {
					t.Errorf("Page %d is not last but has 0 items", pageNum)
				}

				if last {
					break
				}
			}

			// Verify total item count matches original
			if len(allResultKVs) != len(originalKVs) {
				t.Errorf("Expected %d total items, got: %d", len(originalKVs), len(allResultKVs))
			}

			// Verify order and values match original exactly
			for i, resultKV := range allResultKVs {
				if i >= len(originalKVs) {
					t.Errorf("Result has more items than original at index %d", i)
					break
				}

				originalKV := originalKVs[i]
				if !bytes.Equal(resultKV.key, originalKV.key) {
					t.Errorf("Item %d: key mismatch, expected '%s', got '%s'",
						i, string(originalKV.key), string(resultKV.key))
				}
				if len(resultKV.val) != len(originalKV.val) {
					t.Errorf("Item %d: val length mismatch, expected %d, got %d",
						i, len(originalKV.val), len(resultKV.val))
				}
				// Check first few bytes of value for consistency
				for j := 0; j < len(originalKV.val) && j < len(resultKV.val) && j < 3; j++ {
					if resultKV.val[j] != originalKV.val[j] {
						t.Errorf("Item %d: val[%d] mismatch, expected %d, got %d",
							i, j, originalKV.val[j], resultKV.val[j])
						break
					}
				}
			}

			t.Logf("%s: %d items -> %d pages", tc.desc, tc.count, pageNum)
		})
	}
}

func TestLayout_BranchItems_Empty(t *testing.T) {
	items := newMockBranchItems(0, 5, 100)
	next, stop := layout(items, 1024)
	defer stop()

	size, resultItems, last := next()

	// Empty items should return single empty result with HeadSize
	if !last {
		t.Errorf("Expected last=true for empty items")
	}
	if size != HeadSize {
		t.Errorf("Expected size=%d (HeadSize) for empty items, got: %d", HeadSize, size)
	}

	// Result items should be empty
	count := 0
	for range resultItems {
		count++
	}
	if count != 0 {
		t.Errorf("Expected 0 items in result, got: %d", count)
	}
}

func TestLayout_BranchItems_Various(t *testing.T) {
	testCases := []struct {
		name     string
		count    int
		keyLen   int
		pageSize int
		startID  BlockID
		desc     string
	}{
		{"single_item", 1, 5, 1024, 100, "single branch item in large page"},
		{"fits_one_page", 8, 4, 1024, 100, "multiple branch items fit in one page"},
		{"exact_one_page", 50, 4, 512, 100, "branch items exactly fill one page"}, // HeadSize(4) + 50*10 = 504, use 512 as minimum
		{"two_pages", 102, 4, 512, 100, "branch items require exactly two pages"}, // 50 items per page
		{"multiple_pages", 200, 6, 512, 100, "branch items require multiple pages"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Generate mock items and cache original key-BlockID pairs
			items := newMockBranchItems(tc.count, tc.keyLen, tc.startID)
			var originalKPs []struct {
				key     []byte
				blockID BlockID
			}
			for key, blockID := range items {
				originalKPs = append(originalKPs, struct {
					key     []byte
					blockID BlockID
				}{key, blockID})
			}

			// Apply layout
			next, stop := layout(items, tc.pageSize)
			defer stop()

			// Collect all items from all pages in order
			var allResultKPs []struct {
				key     []byte
				blockID BlockID
			}
			pageNum := 0

			for {
				size, resultItems, last := next()
				pageNum++

				// Verify page size doesn't exceed limit
				if size > tc.pageSize {
					t.Errorf("Page %d size=%d exceeds pageSize=%d", pageNum, size, tc.pageSize)
				}

				// Verify page size calculation
				expectedSize := HeadSize
				pageItemCount := 0
				for key, blockID := range resultItems {
					expectedSize += branchItemSize(len(key))
					pageItemCount++
					allResultKPs = append(allResultKPs, struct {
						key     []byte
						blockID BlockID
					}{key, blockID})

					// Verify BlockID >= 2
					if blockID < 2 {
						t.Errorf("Page %d: BlockID=%d should be >= 2", pageNum, blockID)
					}
				}

				if size != expectedSize {
					t.Errorf("Page %d: expected size=%d, got: %d", pageNum, expectedSize, size)
				}

				// Non-last pages should have at least one item
				if !last && pageItemCount == 0 {
					t.Errorf("Page %d is not last but has 0 items", pageNum)
				}

				if last {
					break
				}
			}

			// Verify total item count matches original
			if len(allResultKPs) != len(originalKPs) {
				t.Errorf("Expected %d total items, got: %d", len(originalKPs), len(allResultKPs))
			}

			// Verify order and values match original exactly
			for i, resultKP := range allResultKPs {
				if i >= len(originalKPs) {
					t.Errorf("Result has more items than original at index %d", i)
					break
				}

				originalKP := originalKPs[i]
				if !bytes.Equal(resultKP.key, originalKP.key) {
					t.Errorf("Item %d: key mismatch, expected '%s', got '%s'",
						i, string(originalKP.key), string(resultKP.key))
				}
				if resultKP.blockID != originalKP.blockID {
					t.Errorf("Item %d: BlockID mismatch, expected %d, got %d",
						i, originalKP.blockID, resultKP.blockID)
				}
			}

			t.Logf("%s: %d items -> %d pages", tc.desc, tc.count, pageNum)
		})
	}
}
