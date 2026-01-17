package heap

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/dacapoday/smol/mem"
)

// helper to create heap with mem.File
func newTestHeap(t *testing.T, opt testOption) (*Heap[*mem.File], *mem.File, Checkpoint) {
	t.Helper()
	file := new(mem.File)
	var heap Heap[*mem.File]
	_, ckpt, err := heap.Load(file, opt)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	return &heap, file, ckpt
}

var defaultOpt = testOption{
	cipherSuite:       "crc32",
	magicCode:         [4]byte{'t', 'e', 's', 't'},
	retainCheckpoints: 1,
}

func TestHeapLoadClose(t *testing.T) {
	heap, _, ckpt := newTestHeap(t, defaultOpt)

	if heap.Error() != nil {
		t.Errorf("expected no error, got %v", heap.Error())
	}

	ckpt.Release()
	if !heap.AllCheckpointReleased() {
		t.Error("checkpoint should be released")
	}

	heap.Close()
	if heap.Error() != ErrClosed {
		t.Errorf("expected ErrClosed, got %v", heap.Error())
	}
}

func TestHeapCommit(t *testing.T) {
	heap, file, ckpt := newTestHeap(t, defaultOpt)
	ckpt.Release()

	entry := []byte("test-entry")
	meta, ckpt, err := heap.Commit(entry)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	if !bytes.Equal(meta.Entry, entry) {
		t.Errorf("entry mismatch")
	}
	ckpt.Release()

	// backup file before close
	var backup bytes.Buffer
	file.WriteTo(&backup)
	heap.Close()

	// restore and reload
	file.ReadFrom(&backup)
	var heap2 Heap[*mem.File]
	meta, ckpt, err = heap2.Load(file, defaultOpt)
	if err != nil {
		t.Fatalf("reload failed: %v", err)
	}
	if !bytes.Equal(meta.Entry, entry) {
		t.Errorf("entry after reload mismatch")
	}
	ckpt.Release()
	heap2.Close()
}

func TestHeapCommitEntrySizes(t *testing.T) {
	sizes := []int{0, 10, 100, 500, 1000, 2000}

	for _, size := range sizes {
		file := new(mem.File)
		var heap Heap[*mem.File]
		_, ckpt, _ := heap.Load(file, defaultOpt)
		ckpt.Release()

		entry := make([]byte, size)
		rand.Read(entry)

		meta, ckpt, err := heap.Commit(entry)
		if err != nil {
			t.Fatalf("size %d: Commit failed: %v", size, err)
		}
		if !bytes.Equal(meta.Entry, entry) {
			t.Errorf("size %d: entry mismatch at commit", size)
		}
		ckpt.Release()

		// backup before close
		var backup bytes.Buffer
		file.WriteTo(&backup)
		heap.Close()

		// reload
		file.ReadFrom(&backup)
		var heap2 Heap[*mem.File]
		meta, ckpt, _ = heap2.Load(file, defaultOpt)
		if !bytes.Equal(meta.Entry, entry) {
			t.Errorf("size %d: entry mismatch after reload", size)
		}
		ckpt.Release()
		heap2.Close()
	}
}

func TestHeapReadWriteBlock(t *testing.T) {
	heap, _, ckpt := newTestHeap(t, defaultOpt)
	ckpt.Release()

	bid, _ := heap.Allocate()
	if bid < 2 {
		t.Fatalf("invalid blockID: %d", bid)
	}

	// write
	pageSize := heap.PageSize()
	data := make([]byte, pageSize)
	rand.Read(data)

	buffer := make([]byte, heap.BlockSize())
	copy(buffer, data)
	if err := heap.WriteBlock(bid, buffer); err != nil {
		t.Fatalf("WriteBlock failed: %v", err)
	}

	// read
	readBuf := make([]byte, heap.BlockSize())
	if err := heap.ReadBlock(bid, readBuf); err != nil {
		t.Fatalf("ReadBlock failed: %v", err)
	}
	if !bytes.Equal(data, readBuf[:pageSize]) {
		t.Error("data mismatch")
	}

	heap.Close()
}

func TestHeapAllocateExtend(t *testing.T) {
	heap, _, ckpt := newTestHeap(t, defaultOpt)
	ckpt.Release()

	// allocate extends file
	bid1, reuse := heap.Allocate()
	if bid1 < 2 || reuse {
		t.Errorf("first allocate: bid=%d reuse=%v", bid1, reuse)
	}

	bid2, reuse := heap.Allocate()
	if bid2 <= bid1 || reuse {
		t.Errorf("second allocate: bid=%d reuse=%v", bid2, reuse)
	}

	// extend
	bid3 := heap.Extend()
	if bid3 <= bid2 {
		t.Errorf("extend should return larger blockID: %d <= %d", bid3, bid2)
	}

	heap.Close()
}

func TestHeapRecycle(t *testing.T) {
	// retainCheckpoints=1 creates 2 initial checkpoints
	// Recycled blocks need 3 commits to become available:
	// - 1st commit: records the recycled block
	// - 2nd commit: releases initial checkpoint
	// - 3rd commit: releases the checkpoint holding recycled blocks
	heap, _, ckpt := newTestHeap(t, defaultOpt)
	ckpt.Release()

	bid1, _ := heap.Allocate()
	heap.Allocate() // bid2

	heap.Recycle(bid1)

	// commit 3 times to advance checkpoint chain
	for i := 0; i < 3; i++ {
		_, ckpt, _ := heap.Commit([]byte{byte(i)})
		ckpt.Release()
	}

	// now bid1 should be available for reuse
	bid3, reuse := heap.Allocate()
	if !reuse || bid3 != bid1 {
		t.Errorf("expected reuse bid1(%d), got bid3=%d reuse=%v", bid1, bid3, reuse)
	}

	heap.Close()
}

func TestHeapRecycleMultipleCommits(t *testing.T) {
	// retainCheckpoints=3: needs multiple commits before reuse
	opt := testOption{
		cipherSuite:       "crc32",
		magicCode:         [4]byte{'t', 'e', 's', 't'},
		retainCheckpoints: 3,
	}
	heap, _, ckpt := newTestHeap(t, opt)
	ckpt.Release()

	bid1, _ := heap.Allocate()
	heap.Recycle(bid1)

	// commit multiple times to advance checkpoint chain
	for i := 0; i < 5; i++ {
		_, ckpt, _ := heap.Commit([]byte{byte(i)})
		ckpt.Release()
	}

	// now should be able to reuse bid1
	bid, reuse := heap.Allocate()
	if !reuse || bid != bid1 {
		t.Errorf("expected reuse bid1(%d), got bid=%d reuse=%v", bid1, bid, reuse)
	}

	heap.Close()
}

func TestHeapReadOnly(t *testing.T) {
	file := new(mem.File)

	// create and commit
	var heap Heap[*mem.File]
	_, ckpt, _ := heap.Load(file, defaultOpt)
	ckpt.Release()
	_, ckpt, _ = heap.Commit([]byte("data"))
	ckpt.Release()

	// backup before close
	var backup bytes.Buffer
	file.WriteTo(&backup)
	heap.Close()

	// reopen readonly
	file.ReadFrom(&backup)
	opt := defaultOpt
	opt.readOnly = true
	var heap2 Heap[*mem.File]
	_, ckpt, err := heap2.Load(file, opt)
	if err != nil {
		t.Fatalf("readonly load failed: %v", err)
	}
	ckpt.Release()

	if heap2.Error() != ErrReadOnly {
		t.Errorf("expected ErrReadOnly, got %v", heap2.Error())
	}

	_, err = heap2.WriteAt([]byte("x"), 2)
	if err != ErrReadOnly {
		t.Errorf("WriteAt should fail: %v", err)
	}

	_, _, err = heap2.Commit([]byte("x"))
	if err != ErrReadOnly {
		t.Errorf("Commit should fail: %v", err)
	}

	heap2.Close()
}

func TestHeapRollback(t *testing.T) {
	heap, _, ckpt := newTestHeap(t, defaultOpt)
	ckpt.Release()

	// commit first version
	_, ckpt, _ = heap.Commit([]byte("v1"))
	ckpt.Release()

	// allocate some blocks
	bid1, _ := heap.Allocate()
	heap.Allocate()

	// rollback
	if err := heap.Rollback(); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// after rollback, allocated blocks are recycled via checkpoint mechanism
	// need multiple commits to advance checkpoint chain
	for i := 0; i < 4; i++ {
		_, ckpt, _ := heap.Commit([]byte{byte(i)})
		ckpt.Release()
	}

	bid, reuse := heap.Allocate()
	if !reuse {
		t.Error("block should be reused after rollback")
	}
	_ = bid
	_ = bid1

	heap.Close()
}

func TestHeapCipherAESGCM(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	opt := testOption{
		cipherSuite:       "aes-256-gcm",
		magicCode:         [4]byte{'a', 'e', 's', 't'},
		retainCheckpoints: 1,
		cipherKey:         key,
	}

	file := new(mem.File)
	var heap Heap[*mem.File]
	_, ckpt, err := heap.Load(file, opt)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	ckpt.Release()

	entry := []byte("secret data")
	_, ckpt, err = heap.Commit(entry)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	ckpt.Release()

	// backup before close
	var backup bytes.Buffer
	file.WriteTo(&backup)
	heap.Close()

	// reload with same key
	file.ReadFrom(&backup)
	var heap2 Heap[*mem.File]
	meta, ckpt, err := heap2.Load(file, opt)
	if err != nil {
		t.Fatalf("reload failed: %v", err)
	}
	if !bytes.Equal(meta.Entry, entry) {
		t.Error("entry mismatch after reload")
	}
	ckpt.Release()

	// backup again before close
	backup.Reset()
	file.WriteTo(&backup)
	heap2.Close()

	// reload with wrong key should fail
	file.ReadFrom(&backup)
	wrongKey := make([]byte, 32)
	rand.Read(wrongKey)
	opt.cipherKey = wrongKey

	var heap3 Heap[*mem.File]
	_, _, err = heap3.Load(file, opt)
	if err == nil {
		t.Error("load with wrong key should fail")
	}
}

func TestHeapPlainMode(t *testing.T) {
	opt := testOption{
		cipherSuite:       "plain",
		magicCode:         [4]byte{'p', 'l', 'a', 'n'},
		retainCheckpoints: 1,
	}

	file := new(mem.File)
	var heap Heap[*mem.File]
	_, ckpt, err := heap.Load(file, opt)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	ckpt.Release()

	entry := []byte("plain data")
	_, ckpt, _ = heap.Commit(entry)
	ckpt.Release()

	// backup before close
	var backup bytes.Buffer
	file.WriteTo(&backup)
	heap.Close()

	// reload
	file.ReadFrom(&backup)
	var heap2 Heap[*mem.File]
	meta, ckpt, _ := heap2.Load(file, opt)
	if !bytes.Equal(meta.Entry, entry) {
		t.Error("entry mismatch")
	}
	ckpt.Release()
	heap2.Close()
}

func TestHeapMultipleCommits(t *testing.T) {
	heap, file, ckpt := newTestHeap(t, defaultOpt)
	ckpt.Release()

	entries := [][]byte{
		[]byte("entry1"),
		[]byte("entry2"),
		[]byte("entry3-longer-data"),
	}

	for _, e := range entries {
		_, ckpt, err := heap.Commit(e)
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
		ckpt.Release()
	}

	// backup before close
	var backup bytes.Buffer
	file.WriteTo(&backup)
	heap.Close()

	// reload should get last entry
	file.ReadFrom(&backup)
	var heap2 Heap[*mem.File]
	meta, ckpt, _ := heap2.Load(file, defaultOpt)
	if !bytes.Equal(meta.Entry, entries[len(entries)-1]) {
		t.Errorf("expected last entry, got %q", meta.Entry)
	}
	ckpt.Release()
	heap2.Close()
}
