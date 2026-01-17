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
	head, err := Write(block, data, 10)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	body, err := Read(block, nil, head)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !bytes.Equal(body, data) {
		t.Errorf("Data mismatch: got %d bytes, want %d bytes", len(body), len(data))
	}

	err = Recycle(&b, head)
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
	head, err := Write(block, data, 10)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	body, err := Read(block, nil, head)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !bytes.Equal(body, data) {
		t.Errorf("Data mismatch: got %d bytes, want %d bytes", len(body), len(data))
	}

	err = Recycle(&b, head)
	if err != nil {
		t.Fatalf("Recycle failed: %v", err)
	}
}
