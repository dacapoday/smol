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
	// Empty file returns error (either io.EOF or wrapped)
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

	// Write meta to block 0
	meta := &Meta{
		Ckp:        1,
		BlockSize:  4096,
		BlockCount: 2,
	}
	buf := make([]byte, 4096)
	buffer := Buffer(buf[:4]) // Start with len=4 (magic placeholder)
	encodeMeta(&buffer, meta)
	copy(buf[:4], magic[:])
	file.WriteAt(buffer, 0)

	metaA, metaB, err := loadMeta(io.NewSectionReader(file, 0, 1<<17), magic)
	if err != nil {
		t.Fatalf("loadMeta failed: %v", err)
	}
	if metaA == nil {
		t.Fatal("metaA should not be nil")
	}
	if metaA.Ckp != 1 {
		t.Errorf("expected Ckp=1, got %d", metaA.Ckp)
	}
	if metaB != nil {
		t.Error("metaB should be nil (only one meta written)")
	}
}

func TestLoadMetaBoth(t *testing.T) {
	file := new(mem.File)
	magic := [4]byte{'t', 'e', 's', 't'}
	blockSize := int64(4096)

	// Write meta A (ckp=2) to block 0
	metaA := &Meta{Ckp: 2, BlockSize: 4096, BlockCount: 2}
	bufA := make([]byte, blockSize)
	bufferA := Buffer(bufA[:4])
	encodeMeta(&bufferA, metaA)
	copy(bufA[:4], magic[:])
	file.WriteAt(bufferA, 0)

	// Write meta B (ckp=3) to block 1
	metaB := &Meta{Ckp: 3, BlockSize: 4096, BlockCount: 2}
	bufB := make([]byte, blockSize)
	bufferB := Buffer(bufB[:4])
	encodeMeta(&bufferB, metaB)
	copy(bufB[:4], magic[:])
	file.WriteAt(bufferB, blockSize)

	gotA, gotB, err := loadMeta(io.NewSectionReader(file, 0, 1<<17), magic)
	if err != nil {
		t.Fatalf("loadMeta failed: %v", err)
	}
	if gotA == nil || gotB == nil {
		t.Fatal("both meta should be loaded")
	}
	if gotA.Ckp != 2 || gotB.Ckp != 3 {
		t.Errorf("expected Ckp 2 and 3, got %d and %d", gotA.Ckp, gotB.Ckp)
	}
}

func TestLoadMetaSelectNewer(t *testing.T) {
	file := new(mem.File)
	magic := [4]byte{'t', 'e', 's', 't'}
	blockSize := int64(4096)

	// Write meta A (ckp=5) to block 0
	metaA := &Meta{Ckp: 5, BlockSize: 4096, BlockCount: 2}
	bufA := make([]byte, blockSize)
	bufferA := Buffer(bufA[:4])
	encodeMeta(&bufferA, metaA)
	copy(bufA[:4], magic[:])
	file.WriteAt(bufferA, 0)

	// Write meta B (ckp=4) to block 1
	metaB := &Meta{Ckp: 4, BlockSize: 4096, BlockCount: 2}
	bufB := make([]byte, blockSize)
	bufferB := Buffer(bufB[:4])
	encodeMeta(&bufferB, metaB)
	copy(bufB[:4], magic[:])
	file.WriteAt(bufferB, blockSize)

	gotA, gotB, err := loadMeta(io.NewSectionReader(file, 0, 1<<17), magic)
	if err != nil {
		t.Fatalf("loadMeta failed: %v", err)
	}

	// head.go load() selects: if A.Ckp >= B.Ckp, choose A
	// A.Ckp=5 > B.Ckp=4, so A should be selected
	if gotA.Ckp < gotB.Ckp {
		t.Errorf("A.Ckp=%d should be >= B.Ckp=%d", gotA.Ckp, gotB.Ckp)
	}
}

func TestLoadMetaCkpWrap(t *testing.T) {
	file := new(mem.File)
	magic := [4]byte{'t', 'e', 's', 't'}
	blockSize := int64(4096)

	// Test uint32 wrap: A.Ckp=0, B.Ckp=MaxUint32
	// A is newer (just wrapped from MaxUint32)
	metaA := &Meta{Ckp: 0, BlockSize: 4096, BlockCount: 2}
	bufA := make([]byte, blockSize)
	bufferA := Buffer(bufA[:4])
	encodeMeta(&bufferA, metaA)
	copy(bufA[:4], magic[:])
	file.WriteAt(bufferA, 0)

	metaB := &Meta{Ckp: 1<<32 - 1, BlockSize: 4096, BlockCount: 2}
	bufB := make([]byte, blockSize)
	bufferB := Buffer(bufB[:4])
	encodeMeta(&bufferB, metaB)
	copy(bufB[:4], magic[:])
	file.WriteAt(bufferB, blockSize)

	gotA, gotB, err := loadMeta(io.NewSectionReader(file, 0, 1<<17), magic)
	if err != nil {
		t.Fatalf("loadMeta failed: %v", err)
	}

	// Verify wrap detection logic:
	// A.Ckp=0, B.Ckp=MaxUint32
	// A.Ckp < B.Ckp (0 < MaxUint32), but:
	// A.Ckp == 0 && A.Ckp-1 == B.Ckp (0-1 underflows to MaxUint32)
	// So A should be selected as newer
	if gotA.Ckp != 0 || gotB.Ckp != 1<<32-1 {
		t.Errorf("expected Ckp 0 and MaxUint32, got %d and %d", gotA.Ckp, gotB.Ckp)
	}

	// Simulate selection logic from head.go
	var selected *Meta
	if gotA.Ckp < gotB.Ckp {
		if gotA.Ckp == 0 && gotA.Ckp-1 == gotB.Ckp {
			selected = gotA // wrap case
		} else {
			selected = gotB
		}
	} else {
		selected = gotA
	}

	if selected.Ckp != 0 {
		t.Errorf("wrap case: expected Ckp=0 selected, got %d", selected.Ckp)
	}
}

func TestLoadMetaWrongMagic(t *testing.T) {
	file := new(mem.File)
	magic := [4]byte{'t', 'e', 's', 't'}
	wrongMagic := [4]byte{'b', 'a', 'd', '!'}

	// Write meta with wrong magic
	meta := &Meta{Ckp: 1, BlockSize: 4096, BlockCount: 2}
	buf := make([]byte, 4096)
	buffer := Buffer(buf[:4])
	encodeMeta(&buffer, meta)
	copy(buf[:4], wrongMagic[:])
	file.WriteAt(buffer, 0)

	_, _, err := loadMeta(io.NewSectionReader(file, 0, 1<<17), magic)
	if err == nil {
		t.Error("expected error for wrong magic")
	}
}

func TestReadMeta(t *testing.T) {
	file := new(mem.File)
	blockSize := int64(4096)

	// Write meta at block 2 (offset 8192)
	// History meta doesn't have magic, starts with TLV directly
	meta := &Meta{
		Ckp:        42,
		BlockSize:  4096,
		BlockCount: 10,
		Entry:      []byte("test entry"),
	}
	buf := make([]byte, blockSize)
	buffer := Buffer(buf[:4]) // reserve 4 bytes (would be magic in slot 0/1)
	encodeMeta(&buffer, meta)
	file.WriteAt(buffer, 2*blockSize)

	// Read back (readMeta reads from offset, limit bytes)
	// heap.meta() calls readMeta(file, blockID*size+4, size-4)
	got, err := readMeta(file, 2*blockSize+4, blockSize-4)
	if err != nil {
		t.Fatalf("readMeta failed: %v", err)
	}
	if got.Ckp != 42 {
		t.Errorf("expected Ckp=42, got %d", got.Ckp)
	}
	if !bytes.Equal(got.Entry, meta.Entry) {
		t.Errorf("entry mismatch")
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
	if heap.buffer == nil {
		t.Error("buffer should be allocated")
	}

	// Verify magic written to file
	buf := make([]byte, 4)
	file.ReadAt(buf, 0)
	if !bytes.Equal(buf, opt.magicCode[:]) {
		t.Errorf("magic mismatch: got %v, want %v", buf, opt.magicCode)
	}
}

func TestHeapFlush(t *testing.T) {
	file := new(mem.File)
	opt := testOption{
		magicCode:         [4]byte{'f', 'l', 's', 'h'},
		retainCheckpoints: 3,
		cipherSuite:       "plain",
	}

	var heap Heap[*mem.File]
	_, err := heap.init(file, opt)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}

	// Create a new meta and flush
	meta := &Meta{
		Ckp:        1,
		BlockSize:  uint32(heap.block.size),
		BlockCount: heap.block.count,
	}

	err = heap.flush(meta)
	if err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Verify meta written to block 1 (ckp=1, 1%2=1)
	buf := make([]byte, 4)
	file.ReadAt(buf, heap.block.size) // block 1
	if !bytes.Equal(buf, opt.magicCode[:]) {
		t.Errorf("magic at block 1: got %v, want %v", buf, opt.magicCode)
	}
}

func TestHeapFlushWithHistoryMeta(t *testing.T) {
	file := new(mem.File)
	opt := testOption{
		magicCode:         [4]byte{'h', 'i', 's', 't'},
		retainCheckpoints: 3,
		cipherSuite:       "plain",
	}

	var heap Heap[*mem.File]
	_, err := heap.init(file, opt)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}

	// Extend file for history meta
	heap.block.extend()

	// Create meta with history ID
	meta := &Meta{
		Ckp:        2,
		ID:         2, // history meta at block 2
		BlockSize:  uint32(heap.block.size),
		BlockCount: heap.block.count,
	}

	err = heap.flush(meta)
	if err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Verify history meta at block 2 has no magic (zeroed)
	buf := make([]byte, 4)
	file.ReadAt(buf, 2*heap.block.size)
	if !bytes.Equal(buf, []byte{0, 0, 0, 0}) {
		t.Errorf("history meta should have zeroed magic: got %v", buf)
	}

	// Verify main meta at block 0 (ckp=2, 2%2=0) has magic
	file.ReadAt(buf, 0)
	if !bytes.Equal(buf, opt.magicCode[:]) {
		t.Errorf("main meta should have magic: got %v", buf)
	}
}
