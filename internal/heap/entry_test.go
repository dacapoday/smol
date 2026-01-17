package heap

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/dacapoday/smol/mem"
)

func TestEncodeEntryPlain(t *testing.T) {
	var c codec
	c.aead = plainAEAD{castagnoliCrcTable}
	c.spec = nil

	entry := []byte("test entry data")
	encoded := c.encodeEntry(entry)

	// crc32-plain mode: returns entry unchanged
	if !bytes.Equal(encoded, entry) {
		t.Errorf("plain mode should return entry unchanged")
	}
}

func TestEncodeEntryCRC32(t *testing.T) {
	var c codec
	c.aead = crc32AEAD{castagnoliCrcTable}
	c.spec = []byte{}

	entry := []byte("test entry data")
	encoded := c.encodeEntry(entry)

	// crc32 mode: returns entry + 4 bytes overhead
	if len(encoded) != len(entry)+c.size() {
		t.Errorf("expected len %d, got %d", len(entry)+c.size(), len(encoded))
	}
}

func TestEncodeEntryAESGCM(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	var c codec
	c.aead, _ = aesGCMAEAD(key)
	c.spec = []byte{0x0a}

	entry := []byte("test entry data")
	encoded := c.encodeEntry(entry)

	// aes-gcm mode: returns entry + nonce(12) + tag(16) = entry + 28
	if len(encoded) != len(entry)+c.size() {
		t.Errorf("expected len %d, got %d", len(entry)+c.size(), len(encoded))
	}

	// Verify encoded data differs from plaintext
	if bytes.Equal(encoded[:len(entry)], entry) {
		t.Error("encrypted data should differ from plaintext")
	}
}

func TestLoadEntryInlineCRC32(t *testing.T) {
	var c codec
	c.aead = crc32AEAD{castagnoliCrcTable}
	c.spec = []byte{}

	entry := []byte("small entry")
	encoded := c.encodeEntry(entry)

	meta := &Meta{
		CodecSpec: []byte{},
		BlockSize: 4096,
		EntrySize: uint32(len(entry)),
		EntryID:   0, // inline
		Entry:     encoded,
	}

	file := new(mem.File)
	err := c.loadEntry(file, meta)
	if err != nil {
		t.Fatalf("loadEntry failed: %v", err)
	}

	if !bytes.Equal(meta.Entry, entry) {
		t.Errorf("entry mismatch: got %q, want %q", meta.Entry, entry)
	}
}

func TestLoadEntryInlineAESGCM(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	var c codec
	c.aead, _ = aesGCMAEAD(key)
	c.spec = []byte{0x0a}

	entry := []byte("small encrypted entry")
	encoded := c.encodeEntry(entry)

	meta := &Meta{
		CodecSpec: []byte{0x0a},
		BlockSize: 4096,
		EntrySize: uint32(len(entry)),
		EntryID:   0, // inline
		Entry:     encoded,
	}

	file := new(mem.File)
	err := c.loadEntry(file, meta)
	if err != nil {
		t.Fatalf("loadEntry failed: %v", err)
	}

	if !bytes.Equal(meta.Entry, entry) {
		t.Errorf("entry mismatch: got %q, want %q", meta.Entry, entry)
	}
}

func TestLoadPlainEntryInline(t *testing.T) {
	entry := []byte("plain inline entry")

	meta := &Meta{
		CodecSpec: nil,
		BlockSize: 4096,
		EntrySize: uint32(len(entry)),
		EntryID:   0,
		Entry:     entry,
	}

	file := new(mem.File)
	err := loadPlainEntry(file, meta)
	if err != nil {
		t.Fatalf("loadPlainEntry failed: %v", err)
	}

	if !bytes.Equal(meta.Entry, entry) {
		t.Errorf("entry mismatch")
	}
}

func TestSaveAndLoadEntryCRC32(t *testing.T) {
	file := new(mem.File)

	// Initialize heap
	opt := testOption{
		magicCode:         [4]byte{'e', 'n', 't', 'r'},
		retainCheckpoints: 3,
		cipherSuite:       "crc32",
	}

	var heap Heap[*mem.File]
	_, ckpt, err := heap.Load(file, opt)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	ckpt.Release()

	// Create large entry that needs overflow
	largeEntry := make([]byte, heap.PageSize()-100)
	for i := range largeEntry {
		largeEntry[i] = byte(i % 256)
	}

	meta := &Meta{
		CodecSpec: heap.codec.spec,
		BlockSize: uint32(heap.block.size),
		EntrySize: uint32(len(largeEntry)),
		EntryID:   2, // use block 2 for entry
		Entry:     heap.codec.encodeEntry(largeEntry),
	}

	// Extend file and save entry
	heap.block.extend()
	err = heap.saveEntry(meta)
	if err != nil {
		t.Fatalf("saveEntry failed: %v", err)
	}

	// Load entry back
	var c2 codec
	c2.aead = crc32AEAD{castagnoliCrcTable}
	c2.spec = []byte{}

	meta2 := &Meta{
		CodecSpec: []byte{},
		BlockSize: uint32(heap.block.size),
		EntrySize: uint32(len(largeEntry)),
		EntryID:   2,
		Entry:     meta.Entry,
	}

	err = c2.loadEntry(file, meta2)
	if err != nil {
		t.Fatalf("loadEntry failed: %v", err)
	}

	if !bytes.Equal(meta2.Entry, largeEntry) {
		t.Errorf("entry mismatch after save/load")
	}
}

func TestSaveAndLoadPlainEntry(t *testing.T) {
	file := new(mem.File)

	// Initialize heap with plain mode
	opt := testOption{
		magicCode:         [4]byte{'p', 'l', 'a', 'n'},
		retainCheckpoints: 3,
		cipherSuite:       "plain",
	}

	var heap Heap[*mem.File]
	_, ckpt, err := heap.Load(file, opt)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	ckpt.Release()

	// Create entry that needs overflow
	entry := make([]byte, heap.PageSize()-50)
	for i := range entry {
		entry[i] = byte(i % 256)
	}

	// Extend file
	heap.block.extend()

	meta := &Meta{
		CodecSpec: nil,
		BlockSize: uint32(heap.block.size),
		EntrySize: uint32(len(entry)),
		EntryID:   2,
		Entry:     entry,
	}

	err = heap.savePlainEntry(meta)
	if err != nil {
		t.Fatalf("savePlainEntry failed: %v", err)
	}

	// Load entry back
	meta2 := &Meta{
		CodecSpec: nil,
		BlockSize: uint32(heap.block.size),
		EntrySize: uint32(len(entry)),
		EntryID:   2,
		Entry:     meta.Entry, // overflow portion
	}

	err = loadPlainEntry(file, meta2)
	if err != nil {
		t.Fatalf("loadPlainEntry failed: %v", err)
	}

	if !bytes.Equal(meta2.Entry, entry) {
		t.Errorf("entry mismatch after save/load")
	}
}

func TestEntryHeaderFormat(t *testing.T) {
	file := new(mem.File)

	opt := testOption{
		magicCode:         [4]byte{'h', 'd', 'r', 't'},
		retainCheckpoints: 3,
		cipherSuite:       "crc32",
	}

	var heap Heap[*mem.File]
	_, ckpt, err := heap.Load(file, opt)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	ckpt.Release()

	// Extend file
	heap.block.extend()

	entry := []byte("test entry for header check")
	meta := &Meta{
		CodecSpec: heap.codec.spec,
		BlockSize: uint32(heap.block.size),
		EntrySize: uint32(len(entry)),
		EntryID:   2,
		Entry:     heap.codec.encodeEntry(entry),
	}

	err = heap.saveEntry(meta)
	if err != nil {
		t.Fatalf("saveEntry failed: %v", err)
	}

	// Read raw header from file
	header := make([]byte, 4)
	file.ReadAt(header, 2*heap.block.size)

	// Header should be [00 00 len:2]
	if header[0] != 0 || header[1] != 0 {
		t.Errorf("header should start with 00 00, got %x %x", header[0], header[1])
	}
}

func TestLoadEntryInvalidEntryID(t *testing.T) {
	var c codec
	c.aead = crc32AEAD{castagnoliCrcTable}
	c.spec = []byte{}

	meta := &Meta{
		CodecSpec: []byte{},
		BlockSize: 4096,
		EntrySize: 100,  // says 100 bytes
		EntryID:   0,    // but no EntryID
		Entry:     nil,  // and no inline data
	}

	file := new(mem.File)
	err := c.loadEntry(file, meta)
	if err == nil {
		t.Error("expected error for invalid entry configuration")
	}
}
