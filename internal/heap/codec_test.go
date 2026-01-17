package heap

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/dacapoday/smol/mem"
)

func TestCodecCRC32PlainEncode(t *testing.T) {
	var c codec
	c.aead = plainAEAD{castagnoliCrcTable}

	data := []byte("hello world")
	buffer := make([]byte, len(data)+c.size())
	copy(buffer, data)

	c.encode(buffer, 42)

	if len(buffer) != len(data)+4 {
		t.Errorf("expected buffer length %d, got %d", len(data)+4, len(buffer))
	}

	err := c.decode(buffer, 42)
	if err != nil {
		t.Errorf("decode failed: %v", err)
	}

	if !bytes.Equal(buffer[:len(data)], data) {
		t.Errorf("data mismatch after decode")
	}
}

func TestCodecCRC32PlainInvalidChecksum(t *testing.T) {
	var c codec
	c.aead = plainAEAD{castagnoliCrcTable}

	data := []byte("hello world")
	buffer := make([]byte, len(data)+c.size())
	copy(buffer, data)

	c.encode(buffer, 42)
	buffer[len(buffer)-1] ^= 0xFF

	err := c.decode(buffer, 42)
	if err != ErrInvalidChecksum {
		t.Errorf("expected ErrInvalidChecksum, got %v", err)
	}
}

func TestCodecCRC32Encode(t *testing.T) {
	var c codec
	c.aead = crc32AEAD{castagnoliCrcTable}
	c.spec = []byte{}

	data := []byte("test data with auth")
	buffer := make([]byte, len(data)+c.size())
	copy(buffer, data)

	blockID := BlockID(100)
	c.encode(buffer, blockID)

	err := c.decode(buffer, blockID)
	if err != nil {
		t.Errorf("decode failed: %v", err)
	}

	if !bytes.Equal(buffer[:len(data)], data) {
		t.Errorf("data mismatch after decode")
	}
}

func TestCodecCRC32WrongBlockID(t *testing.T) {
	var c codec
	c.aead = crc32AEAD{castagnoliCrcTable}
	c.spec = []byte{}

	data := []byte("test")
	buffer := make([]byte, len(data)+c.size())
	copy(buffer, data)

	c.encode(buffer, 100)

	err := c.decode(buffer, 101)
	if err != ErrInvalidChecksum {
		t.Errorf("expected ErrInvalidChecksum with wrong blockID, got %v", err)
	}
}

func TestCodecAESGCMEncode(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	aead, err := aesGCMAEAD(key)
	if err != nil {
		t.Fatalf("failed to create AES-GCM: %v", err)
	}

	var c codec
	c.aead = aead
	c.spec = []byte{0x02}

	data := []byte("secret message")
	buffer := make([]byte, len(data)+c.size())
	copy(buffer, data)

	blockID := BlockID(42)
	c.encode(buffer, blockID)

	if bytes.Equal(buffer[:len(data)], data) {
		t.Error("encrypted data should not match plaintext")
	}

	err = c.decode(buffer, blockID)
	if err != nil {
		t.Errorf("decode failed: %v", err)
	}

	if !bytes.Equal(buffer[:len(data)], data) {
		t.Errorf("decrypted data mismatch")
	}
}

func TestCodecAESGCMWrongKey(t *testing.T) {
	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	rand.Read(key1)
	rand.Read(key2)

	aead1, _ := aesGCMAEAD(key1)
	aead2, _ := aesGCMAEAD(key2)

	var c1, c2 codec
	c1.aead = aead1
	c2.aead = aead2

	data := []byte("secret")
	buffer := make([]byte, len(data)+c1.size())
	copy(buffer, data)

	c1.encode(buffer, 42)

	err := c2.decode(buffer, 42)
	if err == nil {
		t.Error("expected decode to fail with wrong key")
	}
}

func TestCodecSize(t *testing.T) {
	tests := []struct {
		name         string
		codec        codec
		expectedSize int
	}{
		{
			name:         "crc32-plain",
			codec:        codec{aead: plainAEAD{castagnoliCrcTable}},
			expectedSize: 4,
		},
		{
			name:         "crc32",
			codec:        codec{aead: crc32AEAD{castagnoliCrcTable}, spec: []byte{}},
			expectedSize: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := tt.codec.size()
			if size != tt.expectedSize {
				t.Errorf("expected size %d, got %d", tt.expectedSize, size)
			}
		})
	}
}

func TestCodecInitOptions(t *testing.T) {
	tests := []struct {
		name        string
		opt         testOption
		expectError bool
		checkSpec   func([]byte) bool
	}{
		{
			name:        "plain",
			opt:         testOption{cipherSuite: "plain"},
			expectError: false,
			checkSpec:   func(spec []byte) bool { return spec == nil },
		},
		{
			name:        "crc32",
			opt:         testOption{cipherSuite: "crc32"},
			expectError: false,
			checkSpec:   func(spec []byte) bool { return spec != nil && len(spec) == 0 },
		},
		{
			name:        "aes-256-gcm with key",
			opt:         testOption{cipherSuite: "aes-256-gcm", cipherKey: make([]byte, 32)},
			expectError: false,
			checkSpec:   func(spec []byte) bool { return len(spec) > 0 },
		},
		{
			name:        "aes-256-gcm without key",
			opt:         testOption{cipherSuite: "aes-256-gcm"},
			expectError: true,
			checkSpec:   nil,
		},
		{
			name:        "invalid suite",
			opt:         testOption{cipherSuite: "invalid"},
			expectError: true,
			checkSpec:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var c codec
			file := new(mem.File)
			meta, err := c.init(file, tt.opt)

			if tt.expectError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if meta == nil {
				t.Error("meta should not be nil")
				return
			}

			if tt.checkSpec != nil && !tt.checkSpec(c.spec) {
				t.Errorf("spec check failed for %v", c.spec)
			}
		})
	}
}

func TestCodecLoadPlain(t *testing.T) {
	// Test loading codec with nil spec (crc32-plain mode)
	meta := &Meta{
		CodecSpec:  nil,
		BlockSize:  4096,
		EntrySize:  0,
		EntryID:    0,
		Entry:      nil,
		BlockCount: 2,
	}

	var c codec
	file := new(mem.File)
	err := c.load(file, testOption{}, meta)
	if err != nil {
		t.Errorf("load plain spec failed: %v", err)
	}
	if c.spec != nil {
		t.Errorf("expected nil spec, got %v", c.spec)
	}
	if c.size() != 4 {
		t.Errorf("expected size 4, got %d", c.size())
	}
}

func TestCodecLoadCRC32(t *testing.T) {
	// Test loading codec with empty spec (crc32 mode)
	// For crc32 mode (spec != nil), entry must be encoded with codec
	var c codec
	c.aead = crc32AEAD{castagnoliCrcTable}
	c.spec = []byte{}
	encoded := c.encodeEntry(nil) // empty entry

	meta := &Meta{
		CodecSpec:  []byte{},
		BlockSize:  4096,
		EntrySize:  0,
		EntryID:    0,
		Entry:      encoded,
		BlockCount: 2,
	}

	var c2 codec
	file := new(mem.File)
	err := c2.load(file, testOption{}, meta)
	if err != nil {
		t.Errorf("load crc32 spec failed: %v", err)
	}
	if c2.spec == nil || len(c2.spec) != 0 {
		t.Errorf("expected empty spec, got %v", c2.spec)
	}
	if c2.size() != 4 {
		t.Errorf("expected size 4, got %d", c2.size())
	}
}

func TestCodecLoadAESGCM(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	// Prepare entry with overhead
	entry := []byte("test entry")
	var c codec
	c.aead, _ = aesGCMAEAD(key)
	c.spec = []byte{0x0a} // varint(5) = 0x0a
	encoded := c.encodeEntry(entry)

	meta := &Meta{
		CodecSpec:  []byte{0x0a}, // varint(5) for aes-256-gcm
		BlockSize:  4096,
		EntrySize:  uint32(len(entry)),
		EntryID:    0,
		Entry:      encoded,
		BlockCount: 2,
	}

	var c2 codec
	file := new(mem.File)
	err := c2.load(file, testOption{cipherKey: key}, meta)
	if err != nil {
		t.Errorf("load aes-gcm spec failed: %v", err)
	}
	if !bytes.Equal(meta.Entry, entry) {
		t.Errorf("entry mismatch after load")
	}
}

func TestCodecLoadAESGCMWrongKey(t *testing.T) {
	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	rand.Read(key1)
	rand.Read(key2)

	// Prepare entry with key1
	entry := []byte("test entry")
	var c codec
	c.aead, _ = aesGCMAEAD(key1)
	c.spec = []byte{0x0a}
	encoded := c.encodeEntry(entry)

	meta := &Meta{
		CodecSpec:  []byte{0x0a},
		BlockSize:  4096,
		EntrySize:  uint32(len(entry)),
		EntryID:    0,
		Entry:      encoded,
		BlockCount: 2,
	}

	// Try to load with key2
	var c2 codec
	file := new(mem.File)
	err := c2.load(file, testOption{cipherKey: key2}, meta)
	if err == nil {
		t.Error("expected error with wrong key")
	}
}
