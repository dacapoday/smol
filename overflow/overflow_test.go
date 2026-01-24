package overflow

import (
	"bytes"
	"testing"

	"github.com/dacapoday/smol/block"
	"github.com/dacapoday/smol/mem"
)

type option struct{}

func (o option) MagicCode() [4]byte          { return [4]byte{'o', 'v', 'e', 'r'} }
func (o option) ReadOnly() bool              { return false }
func (o option) IgnoreInvalidFreelist() bool { return false }
func (o option) RetainCheckpoints() uint8    { return 0 }
func (o option) BlockSize() int              { return 512 }

func TestOverflowWriteReadRecycle(t *testing.T) {
	var f mem.File
	file := &f

	var b block.Heap[*mem.File]
	block := &b

	_, ckpt, err := block.Load(file, option{})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	defer ckpt.Release()
	defer block.Close()

	data := bytes.Repeat([]byte("hello world"), 2048)
	head, overflowSize, overflowID, err := Write(block, data, 10)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	body, err := Read(block, nil, head, overflowSize, overflowID)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !bytes.Equal(body, data) {
		t.Errorf("Data mismatch: got %d bytes, want %d bytes", len(body), len(data))
	}

	err = Recycle(&b, overflowID)
	if err != nil {
		t.Fatalf("Recycle failed: %v", err)
	}
}

func TestOverflowWriteReadRecycleSmallOverflow(t *testing.T) {
	var f mem.File
	file := &f

	var b block.Heap[*mem.File]
	block := &b

	_, ckpt, err := block.Load(file, option{})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	defer ckpt.Release()
	defer block.Close()

	data := []byte("hello world")
	head, overflowSize, overflowID, err := Write(block, data, 10)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	body, err := Read(block, nil, head, overflowSize, overflowID)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !bytes.Equal(body, data) {
		t.Errorf("Data mismatch: got %d bytes, want %d bytes", len(body), len(data))
	}

	err = Recycle(&b, overflowID)
	if err != nil {
		t.Fatalf("Recycle failed: %v", err)
	}
}

func TestIterMultiPage(t *testing.T) {
	var f mem.File
	var b block.Heap[*mem.File]

	_, ckpt, err := b.Load(&f, option{})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	defer ckpt.Release()
	defer b.Close()

	data := bytes.Repeat([]byte("hello world"), 2048)
	head, _, overflowID, err := Write(&b, data, 10)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// collect all pages via Iter
	var collected []byte
	collected = append(collected, head...)
	for chunk, err := range Iter(&b, overflowID) {
		if err != nil {
			t.Fatalf("Iter error: %v", err)
		}
		collected = append(collected, chunk...)
	}

	if !bytes.Equal(collected, data) {
		t.Errorf("Data mismatch: got %d bytes, want %d bytes", len(collected), len(data))
	}
}

func TestIterSinglePage(t *testing.T) {
	var f mem.File
	var b block.Heap[*mem.File]

	_, ckpt, err := b.Load(&f, option{})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	defer ckpt.Release()
	defer b.Close()

	// small overflow: only tail page
	data := []byte("hello world!")
	head, _, overflowID, err := Write(&b, data, 5)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	var collected []byte
	collected = append(collected, head...)
	for chunk, err := range Iter(&b, overflowID) {
		if err != nil {
			t.Fatalf("Iter error: %v", err)
		}
		collected = append(collected, chunk...)
	}

	if !bytes.Equal(collected, data) {
		t.Errorf("Data mismatch: got %q, want %q", collected, data)
	}
}

func TestIterEarlyStop(t *testing.T) {
	var f mem.File
	var b block.Heap[*mem.File]

	_, ckpt, err := b.Load(&f, option{})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	defer ckpt.Release()
	defer b.Close()

	data := bytes.Repeat([]byte("hello world"), 2048)
	_, _, overflowID, err := Write(&b, data, 10)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// stop after first page
	count := 0
	for _, err := range Iter(&b, overflowID) {
		if err != nil {
			t.Fatalf("Iter error: %v", err)
		}
		count++
		break
	}

	if count != 1 {
		t.Errorf("Expected 1 iteration, got %d", count)
	}
}

func TestIterInvalidID(t *testing.T) {
	var f mem.File
	var b block.Heap[*mem.File]

	_, ckpt, err := b.Load(&f, option{})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	defer ckpt.Release()
	defer b.Close()

	// overflowID < 2 should yield nothing
	count := 0
	for _, err := range Iter(&b, 0) {
		if err != nil {
			t.Fatalf("Iter error: %v", err)
		}
		count++
	}

	if count != 0 {
		t.Errorf("Expected 0 iterations for invalid ID, got %d", count)
	}
}
