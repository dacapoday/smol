package heap

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/dacapoday/smol/mem"
)

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

	var backup bytes.Buffer
	file.WriteTo(&backup)
	heap.Close()

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

func TestHeapReadWriteBlock(t *testing.T) {
	heap, _, ckpt := newTestHeap(t, defaultOpt)
	ckpt.Release()

	bid, _ := heap.Allocate()
	if bid < 2 {
		t.Fatalf("invalid blockID: %d", bid)
	}

	pageSize := heap.PageSize()
	data := make([]byte, pageSize)
	rand.Read(data)

	buffer := make([]byte, heap.BlockSize())
	copy(buffer, data)
	if err := heap.WriteBlock(bid, buffer); err != nil {
		t.Fatalf("WriteBlock failed: %v", err)
	}

	readBuf := make([]byte, heap.BlockSize())
	if err := heap.ReadBlock(bid, readBuf); err != nil {
		t.Fatalf("ReadBlock failed: %v", err)
	}
	if !bytes.Equal(data, readBuf[:pageSize]) {
		t.Error("data mismatch")
	}

	heap.Close()
}

func TestHeapAllocateRecycle(t *testing.T) {
	heap, _, ckpt := newTestHeap(t, defaultOpt)
	ckpt.Release()

	bid1, reuse := heap.Allocate()
	if bid1 < 2 || reuse {
		t.Errorf("first allocate: bid=%d reuse=%v", bid1, reuse)
	}

	bid2, _ := heap.Allocate()
	if bid2 <= bid1 {
		t.Errorf("second allocate should be larger: %d <= %d", bid2, bid1)
	}

	heap.Recycle(bid1)

	for i := 0; i < 3; i++ {
		_, ckpt, _ := heap.Commit([]byte{byte(i)})
		ckpt.Release()
	}

	bid3, reuse := heap.Allocate()
	if !reuse || bid3 != bid1 {
		t.Errorf("expected reuse bid1(%d), got bid3=%d reuse=%v", bid1, bid3, reuse)
	}

	heap.Close()
}

func TestHeapRollback(t *testing.T) {
	heap, _, ckpt := newTestHeap(t, defaultOpt)
	ckpt.Release()

	_, ckpt, _ = heap.Commit([]byte("v1"))
	ckpt.Release()

	bid1, _ := heap.Allocate()
	heap.Allocate()

	if err := heap.Rollback(); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

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

func TestHeapClosedMethods(t *testing.T) {
	heap, _, ckpt := newTestHeap(t, defaultOpt)
	ckpt.Release()

	bid, _ := heap.Allocate()
	buffer := make([]byte, heap.BlockSize())

	heap.Close()

	noPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("%s: panicked: %v", name, r)
			}
		}()
		fn()
	}

	noPanic("Error", func() { heap.Error() })
	noPanic("Close", func() { heap.Close() })
	noPanic("AllCheckpointReleased", func() { heap.AllCheckpointReleased() })
	noPanic("Extend", func() { heap.Extend() })
	noPanic("Allocate", func() { heap.Allocate() })
	noPanic("WriteBlock", func() { heap.WriteBlock(bid, buffer) })
	noPanic("WriteAt", func() { heap.WriteAt(buffer, bid) })
	noPanic("Rollback", func() { heap.Rollback() })
	noPanic("Commit", func() { heap.Commit([]byte("test")) })
	noPanic("ReadBlock", func() { heap.ReadBlock(bid, buffer) })
	noPanic("ReadAt", func() { heap.ReadAt(buffer, bid) })
	noPanic("Recycle", func() { heap.Recycle(bid) })
	noPanic("RecycleN", func() {
		heap.RecycleN(func(yield func(BlockID) bool) { yield(bid) })
	})
	noPanic("PageSize", func() { heap.PageSize() })
	noPanic("BlockSize", func() { heap.BlockSize() })
}

func TestHeapReadOnly(t *testing.T) {
	file := new(mem.File)

	var heap Heap[*mem.File]
	_, ckpt, _ := heap.Load(file, defaultOpt)
	ckpt.Release()
	_, ckpt, _ = heap.Commit([]byte("data"))
	ckpt.Release()

	var backup bytes.Buffer
	file.WriteTo(&backup)
	heap.Close()

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
