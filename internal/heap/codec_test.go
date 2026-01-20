package heap

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/dacapoday/smol/mem"
)

func TestCodecPlainAEAD(t *testing.T) {
	var c codec
	c.aead = plainAEAD{castagnoliCrcTable}

	data := []byte("hello world")
	buffer := make([]byte, len(data)+c.size())
	copy(buffer, data)

	c.encode(buffer, 42)
	if err := c.decode(buffer, 42); err != nil {
		t.Errorf("decode failed: %v", err)
	}
	if !bytes.Equal(buffer[:len(data)], data) {
		t.Error("data mismatch")
	}

	buffer[len(buffer)-1] ^= 0xFF
	if err := c.decode(buffer, 42); err != ErrInvalidChecksum {
		t.Errorf("expected ErrInvalidChecksum, got %v", err)
	}
}

func TestCodecCRC32AEAD(t *testing.T) {
	var c codec
	c.aead = crc32AEAD{castagnoliCrcTable}
	c.spec = []byte{}

	data := []byte("test data with auth")
	buffer := make([]byte, len(data)+c.size())
	copy(buffer, data)

	c.encode(buffer, 100)
	if err := c.decode(buffer, 100); err != nil {
		t.Errorf("decode failed: %v", err)
	}
	if !bytes.Equal(buffer[:len(data)], data) {
		t.Error("data mismatch")
	}

	copy(buffer, data)
	c.encode(buffer, 100)
	if err := c.decode(buffer, 101); err != ErrInvalidChecksum {
		t.Errorf("wrong blockID should fail: %v", err)
	}
}

func TestCodecAESGCMAEAD(t *testing.T) {
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

	c.encode(buffer, 42)
	if bytes.Equal(buffer[:len(data)], data) {
		t.Error("encrypted data should differ from plaintext")
	}

	if err := c.decode(buffer, 42); err != nil {
		t.Errorf("decode failed: %v", err)
	}
	if !bytes.Equal(buffer[:len(data)], data) {
		t.Error("decrypted data mismatch")
	}

	key2 := make([]byte, 32)
	rand.Read(key2)
	aead2, _ := aesGCMAEAD(key2)
	c2 := codec{aead: aead2}
	copy(buffer, data)
	c.encode(buffer, 42)
	if err := c2.decode(buffer, 42); err == nil {
		t.Error("wrong key should fail")
	}
}

func TestCodecInit(t *testing.T) {
	tests := []struct {
		name      string
		opt       testOption
		expectErr bool
	}{
		{"plain", testOption{cipherSuite: "plain"}, false},
		{"crc32", testOption{cipherSuite: "crc32"}, false},
		{"aes-256-gcm", testOption{cipherSuite: "aes-256-gcm", cipherKey: make([]byte, 32)}, false},
		{"aes-256-gcm-no-key", testOption{cipherSuite: "aes-256-gcm"}, true},
		{"invalid", testOption{cipherSuite: "invalid"}, true},
	}

	for _, tt := range tests {
		var c codec
		file := new(mem.File)
		_, err := c.init(file, tt.opt)

		if tt.expectErr && err == nil {
			t.Errorf("%s: expected error", tt.name)
		}
		if !tt.expectErr && err != nil {
			t.Errorf("%s: unexpected error: %v", tt.name, err)
		}
	}
}

func TestCodecLoad(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	var c codec
	c.aead, _ = aesGCMAEAD(key)
	c.spec = []byte{0x0a}

	entry := []byte("test entry")
	encoded := c.encodeEntry(entry)

	meta := &Meta{
		CodecSpec:  []byte{0x0a},
		BlockSize:  4096,
		Entry:      encoded,
		BlockCount: 2,
	}

	var c2 codec
	file := new(mem.File)
	if err := c2.load(file, testOption{cipherKey: key}, meta); err != nil {
		t.Errorf("load failed: %v", err)
	}
	if !bytes.Equal(meta.Entry, entry) {
		t.Error("entry mismatch after load")
	}

	key2 := make([]byte, 32)
	rand.Read(key2)
	var c3 codec
	meta.Entry = encoded
	if err := c3.load(file, testOption{cipherKey: key2}, meta); err == nil {
		t.Error("wrong key should fail")
	}
}
