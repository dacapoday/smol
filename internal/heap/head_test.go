package heap

import (
	"bytes"
	"io"
	"testing"

	"github.com/dacapoday/smol/mem"
)

func TestLoadMetaEmpty(t *testing.T) {
	file := new(mem.File)
	magic := [4]byte{'t', 'e', 's', 't'}

	metaA, metaB, err := loadMeta(io.NewSectionReader(file, 0, 1<<17), magic)
	if err == nil {
		t.Error("expected error for empty file")
	}
	if metaA != nil || metaB != nil {
		t.Error("meta should be nil for empty file")
	}
}

func TestLoadMetaSingle(t *testing.T) {
	file := new(mem.File)
	magic := [4]byte{'t', 'e', 's', 't'}

	writeMeta(t, file, 0, magic, &Meta{Ckp: 1, BlockSize: 4096, BlockCount: 2})

	metaA, metaB, err := loadMeta(io.NewSectionReader(file, 0, 1<<17), magic)
	if err != nil {
		t.Fatalf("loadMeta failed: %v", err)
	}
	if metaA == nil || metaA.Ckp != 1 {
		t.Error("metaA should have Ckp=1")
	}
	if metaB != nil {
		t.Error("metaB should be nil")
	}
}

func TestLoadMetaBoth(t *testing.T) {
	file := new(mem.File)
	magic := [4]byte{'t', 'e', 's', 't'}

	writeMeta(t, file, 0, magic, &Meta{Ckp: 2, BlockSize: 4096, BlockCount: 2})
	writeMeta(t, file, 4096, magic, &Meta{Ckp: 3, BlockSize: 4096, BlockCount: 2})

	metaA, metaB, err := loadMeta(io.NewSectionReader(file, 0, 1<<17), magic)
	if err != nil {
		t.Fatalf("loadMeta failed: %v", err)
	}
	if metaA == nil || metaA.Ckp != 2 {
		t.Error("metaA should have Ckp=2")
	}
	if metaB == nil || metaB.Ckp != 3 {
		t.Error("metaB should have Ckp=3")
	}
}

func TestLoadMetaWrongMagic(t *testing.T) {
	file := new(mem.File)
	magic := [4]byte{'t', 'e', 's', 't'}
	wrong := [4]byte{'b', 'a', 'd', '!'}

	writeMeta(t, file, 0, wrong, &Meta{Ckp: 1, BlockSize: 4096, BlockCount: 2})

	_, _, err := loadMeta(io.NewSectionReader(file, 0, 1<<17), magic)
	if err == nil {
		t.Error("expected error for wrong magic")
	}
}

func TestHeapInit(t *testing.T) {
	file := new(mem.File)
	opt := testOption{
		magicCode:         [4]byte{'i', 'n', 'i', 't'},
		retainCheckpoints: 3,
		cipherSuite:       "crc32",
	}

	var heap Heap[*mem.File]
	meta, err := heap.init(file, opt)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}
	if meta.Ckp != 0 {
		t.Errorf("expected Ckp=0, got %d", meta.Ckp)
	}
	if meta.BlockCount != 2 {
		t.Errorf("expected BlockCount=2, got %d", meta.BlockCount)
	}

	buf := make([]byte, 4)
	file.ReadAt(buf, 0)
	if !bytes.Equal(buf, opt.magicCode[:]) {
		t.Errorf("magic mismatch: got %v, want %v", buf, opt.magicCode)
	}
}

func TestHeapFlush(t *testing.T) {
	file := new(mem.File)
	magic := [4]byte{'f', 'l', 's', 'h'}
	opt := testOption{
		magicCode:         magic,
		retainCheckpoints: 3,
		cipherSuite:       "plain",
	}

	var heap Heap[*mem.File]
	heap.init(file, opt)

	meta := &Meta{
		Ckp:        1,
		BlockSize:  uint32(heap.block.size),
		BlockCount: heap.block.count,
	}
	if err := heap.flush(meta); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	_, got, err := loadMeta(io.NewSectionReader(file, 0, 1<<17), magic)
	if err != nil {
		t.Fatalf("loadMeta failed: %v", err)
	}
	if got.Ckp != 1 {
		t.Errorf("expected Ckp=1, got %d", got.Ckp)
	}
}

func TestHeapFlushHistoryMeta(t *testing.T) {
	file := new(mem.File)
	magic := [4]byte{'h', 'i', 's', 't'}
	opt := testOption{
		magicCode:         magic,
		retainCheckpoints: 3,
		cipherSuite:       "plain",
	}

	var heap Heap[*mem.File]
	heap.init(file, opt)
	heap.block.extend()

	meta := &Meta{
		Ckp:        2,
		ID:         2,
		BlockSize:  uint32(heap.block.size),
		BlockCount: heap.block.count,
	}
	if err := heap.flush(meta); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	got, _, err := loadMeta(io.NewSectionReader(file, 0, 1<<17), magic)
	if err != nil {
		t.Fatalf("loadMeta failed: %v", err)
	}
	if got.Ckp != 2 {
		t.Errorf("expected Ckp=2, got %d", got.Ckp)
	}

	history, err := readMeta(file, 2*heap.block.size+4, heap.block.size-4)
	if err != nil {
		t.Fatalf("readMeta failed: %v", err)
	}
	if history.Ckp != 2 {
		t.Errorf("history meta: expected Ckp=2, got %d", history.Ckp)
	}
}

func writeMeta(t *testing.T, file *mem.File, offset int64, magic [4]byte, meta *Meta) {
	t.Helper()
	buf := make([]byte, 4096)
	buffer := Buffer(buf[:4])
	encodeMeta(&buffer, meta)
	copy(buf[:4], magic[:])
	file.WriteAt(buffer, offset)
}
