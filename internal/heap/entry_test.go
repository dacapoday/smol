package heap

import (
	"bytes"
	"testing"

	"github.com/dacapoday/smol/mem"
)

func TestEntryInlineRoundTrip(t *testing.T) {
	var c codec
	c.aead = crc32AEAD{castagnoliCrcTable}
	c.spec = []byte{}

	entry := []byte("small inline entry")
	encoded := c.encodeEntry(entry)

	meta := &Meta{
		CodecSpec: []byte{},
		BlockSize: 4096,
		EntrySize: uint32(len(entry)),
		EntryID:   0,
		Entry:     encoded,
	}

	file := new(mem.File)
	if err := c.loadEntry(file, meta); err != nil {
		t.Fatalf("loadEntry failed: %v", err)
	}
	if !bytes.Equal(meta.Entry, entry) {
		t.Errorf("entry mismatch: got %q, want %q", meta.Entry, entry)
	}
}

func TestEntryPageSizeRoundTrip(t *testing.T) {
	file := new(mem.File)
	opt := testOption{
		magicCode:         [4]byte{'p', 'a', 'g', 'e'},
		retainCheckpoints: 3,
		cipherSuite:       "crc32",
	}

	var heap Heap[*mem.File]
	_, ckpt, err := heap.Load(file, opt)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	ckpt.Release()

	entry := make([]byte, heap.PageSize())
	for i := range entry {
		entry[i] = byte(i % 256)
	}

	_, ckpt, err = heap.Commit(entry)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	ckpt.Release()

	var backup bytes.Buffer
	file.WriteTo(&backup)
	heap.Close()

	file.ReadFrom(&backup)
	var heap2 Heap[*mem.File]
	meta, ckpt, err := heap2.Load(file, opt)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if !bytes.Equal(meta.Entry, entry) {
		t.Errorf("entry mismatch at pageSize boundary")
	}
	ckpt.Release()
	heap2.Close()
}

func TestPlainEntryPageSizeRoundTrip(t *testing.T) {
	file := new(mem.File)
	opt := testOption{
		magicCode:         [4]byte{'p', 'l', 'p', 's'},
		retainCheckpoints: 3,
		cipherSuite:       "plain",
	}

	var heap Heap[*mem.File]
	_, ckpt, err := heap.Load(file, opt)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	ckpt.Release()

	entry := make([]byte, heap.PageSize())
	for i := range entry {
		entry[i] = byte(i % 256)
	}

	_, ckpt, err = heap.Commit(entry)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	ckpt.Release()

	var backup bytes.Buffer
	file.WriteTo(&backup)
	heap.Close()

	file.ReadFrom(&backup)
	var heap2 Heap[*mem.File]
	meta, ckpt, err := heap2.Load(file, opt)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if !bytes.Equal(meta.Entry, entry) {
		t.Errorf("entry mismatch at pageSize boundary")
	}
	ckpt.Release()
	heap2.Close()
}

func TestPlainEntryBlockSizeRoundTrip(t *testing.T) {
	file := new(mem.File)
	opt := testOption{
		magicCode:         [4]byte{'p', 'l', 'p', 's'},
		retainCheckpoints: 3,
		cipherSuite:       "plain",
	}

	var heap Heap[*mem.File]
	_, ckpt, err := heap.Load(file, opt)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	ckpt.Release()

	entry := make([]byte, heap.BlockSize())
	for i := range entry {
		entry[i] = byte(i % 256)
	}

	_, ckpt, err = heap.Commit(entry)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	ckpt.Release()

	var backup bytes.Buffer
	file.WriteTo(&backup)
	heap.Close()

	file.ReadFrom(&backup)
	var heap2 Heap[*mem.File]
	meta, ckpt, err := heap2.Load(file, opt)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if !bytes.Equal(meta.Entry, entry) {
		t.Errorf("entry mismatch at pageSize boundary")
	}
	ckpt.Release()
	heap2.Close()
}
