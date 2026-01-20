package heap

import (
	"bytes"
	"testing"

	"github.com/dacapoday/smol/mem"
)

// TestFreelistChain tests freelist chain write and queue/ring operations.
// When recycled blocks exceed ring capacity (~1021 for 4096 block size),
// saveFreelist is triggered, creating a chain of freelist pages.
// Uses large entries (half page size) to trigger EntryID allocation.
func TestFreelistChain(t *testing.T) {
	opt := testOption{
		cipherSuite:       "crc32",
		magicCode:         [4]byte{'f', 'r', 'e', 'e'},
		retainCheckpoints: 5,
	}

	file := new(mem.File)
	var heap Heap[*mem.File]
	_, ckpt, err := heap.Load(file, opt)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	ckpt.Release()

	// large entry to trigger EntryID allocation (needs > blockSize-256)
	entry := make([]byte, heap.BlockSize()-200)
	for i := range entry {
		entry[i] = byte(i)
	}

	// freelist capacity = (4096 - 12) / 4 = 1021
	// allocate 3000+ blocks to create freelist chain (3+ pages)
	const allocCount = 3500

	ids := make([]BlockID, allocCount)
	for i := range ids {
		ids[i], _ = heap.Allocate()
		if ids[i] < 2 {
			t.Fatalf("allocate %d failed: %v", i, heap.Error())
		}
	}

	// recycle all - triggers multiple saveFreelist calls
	for _, id := range ids {
		heap.Recycle(id)
	}

	// commit with large entry
	meta, ckpt, err := heap.Commit(entry)
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	ckpt.Release()

	if meta.FreeRecycled == 0 {
		t.Error("expected non-zero FreeRecycled")
	}
	t.Logf("after recycle: FreeRecycled=%d, FreeTotal=%d, EntryID=%d",
		meta.FreeRecycled, meta.FreeTotal, meta.EntryID)

	// pass retention window with large entries
	for i := 0; i < 6; i++ {
		entry[0] = byte(i)
		_, ckpt, _ := heap.Commit(entry)
		ckpt.Release()
	}

	// verify reuse from freelist chain (tests queue shift and ring operations)
	reused := 0
	for i := 0; i < allocCount; i++ {
		_, ok := heap.Allocate()
		if ok {
			reused++
		}
	}

	if reused < allocCount/2 {
		t.Errorf("expected more reuse: got %d/%d", reused, allocCount)
	}
	t.Logf("reused %d/%d blocks", reused, allocCount)

	heap.Close()
}

// TestFreelistReload tests loading freelist chain from disk and
// restoring recycled counts per checkpoint correctly.
// Uses large entries to trigger EntryID allocation and recycling.
func TestFreelistReload(t *testing.T) {
	opt := testOption{
		cipherSuite:       "crc32",
		magicCode:         [4]byte{'r', 'l', 'd', 't'},
		retainCheckpoints: 5,
	}

	file := new(mem.File)
	var backup bytes.Buffer
	var blockSize int

	const (
		allocCount   = 2500
		recycleCount = 1200 // > 1021, triggers freelist page
	)

	// phase 1: create freelist chain with multiple commits
	{
		var heap Heap[*mem.File]
		_, ckpt, _ := heap.Load(file, opt)
		ckpt.Release()

		blockSize = heap.BlockSize()
		entry := make([]byte, blockSize-200) // > blockSize-256 triggers EntryID

		ids := make([]BlockID, 0, allocCount)
		for i := 0; i < allocCount; i++ {
			id, _ := heap.Allocate()
			ids = append(ids, id)
		}

		// multiple commits with recycling - creates checkpoint chain with recycled values
		for c := 0; c < 4; c++ {
			n := min(recycleCount, len(ids))
			for i := 0; i < n; i++ {
				heap.Recycle(ids[i])
			}
			ids = ids[n:]

			entry[0] = byte(c)
			meta, ckpt, err := heap.Commit(entry)
			if err != nil {
				t.Fatalf("commit %d failed: %v", c, err)
			}
			ckpt.Release()
			t.Logf("commit %d: FreeRecycled=%d, FreeTotal=%d, EntryID=%d",
				c, meta.FreeRecycled, meta.FreeTotal, meta.EntryID)

			// allocate more for next round
			for i := 0; i < allocCount/2; i++ {
				id, _ := heap.Allocate()
				ids = append(ids, id)
			}
		}

		file.WriteTo(&backup)
		heap.Close()
	}

	// phase 2: reload and verify freelist chain loaded correctly
	{
		file.ReadFrom(&backup)
		var heap Heap[*mem.File]
		meta, ckpt, err := heap.Load(file, opt)
		if err != nil {
			t.Fatalf("reload failed: %v", err)
		}
		ckpt.Release()

		t.Logf("after reload: Ckp=%d, FreeRecycled=%d, FreeTotal=%d",
			meta.Ckp, meta.FreeRecycled, meta.FreeTotal)

		if meta.Ckp == 0 {
			t.Error("expected non-zero Ckp after reload")
		}

		entry := make([]byte, blockSize-200)

		// pass retention window with large entries
		for i := 0; i < 6; i++ {
			entry[0] = byte(i + 10)
			_, ckpt, _ := heap.Commit(entry)
			ckpt.Release()
		}

		// verify blocks can be reused from loaded freelist
		reused := 0
		for i := 0; i < allocCount; i++ {
			_, ok := heap.Allocate()
			if ok {
				reused++
			}
		}

		if reused == 0 {
			t.Error("expected block reuse after reload")
		}
		t.Logf("reused %d/%d blocks after reload", reused, allocCount)

		heap.Close()
	}

	// phase 3: reload again to verify continued operation
	{
		file.ReadFrom(&backup)
		var heap Heap[*mem.File]
		_, ckpt, err := heap.Load(file, opt)
		if err != nil {
			t.Fatalf("second reload failed: %v", err)
		}
		ckpt.Release()

		entry := make([]byte, blockSize-200)

		// allocate and recycle again
		ids := make([]BlockID, 1500)
		for i := range ids {
			ids[i], _ = heap.Allocate()
		}
		for _, id := range ids {
			heap.Recycle(id)
		}

		finalMeta, ckpt, err := heap.Commit(entry)
		if err != nil {
			t.Fatalf("final commit failed: %v", err)
		}
		ckpt.Release()

		t.Logf("after second reload: Ckp=%d, EntryID=%d", finalMeta.Ckp, finalMeta.EntryID)
		heap.Close()
	}
}
