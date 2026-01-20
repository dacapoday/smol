package heap

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"
)

type codec struct {
	aead cipher.AEAD
	spec []byte
}

func (codec *codec) load(file io.ReaderAt, opt Option, meta *Meta) (err error) {
	codec.spec = meta.CodecSpec
	defer func() {
		if err != nil {
			codec.aead = nil
			codec.spec = nil
		}
	}()

	if codec.spec == nil {
		codec.aead = plainAEAD{castagnoliCrcTable}
		return loadPlainEntry(file, meta)
	}

	if len(codec.spec) == 0 {
		codec.aead = crc32AEAD{castagnoliCrcTable}
		return codec.loadEntry(file, meta)
	}

	suite, n := binary.Varint(codec.spec)
	if n <= 0 {
		return fmt.Errorf("%w spec", ErrInvalidCipherSuite)
	}
	switch suite {
	case aes_256_gcm:
		key := getCipherKey(opt)
		if len(key) != 32 {
			return fmt.Errorf("aes-256-gcm: %w size", ErrInvalidCipherKey)
		}

		codec.aead, err = aesGCMAEAD(key)
		if err != nil {
			return fmt.Errorf("aes-256-gcm: %w", err)
		}

		return codec.loadEntry(file, meta)
	}
	return fmt.Errorf("%w cipher suite: %d", ErrUnsupported, suite)
}

func (codec *codec) init(file io.WriterAt, opt Option) (meta *Meta, err error) {
	var blockSize int
	if o, ok := opt.(BlockSize); ok {
		blockSize = o.BlockSize()
	} else {
		blockSize = os.Getpagesize()
	}
	if blockSize > 64*1024 || blockSize < 512 {
		err = fmt.Errorf("%d is %w", blockSize, ErrInvalidBlockSize)
		return
	}
	var blockCount uint32 = 2
	defer func() {
		if err != nil {
			codec.aead = nil
			codec.spec = nil
			return
		}

		meta = &Meta{
			CodecSpec:  codec.spec,
			BlockCount: blockCount,
			BlockSize:  uint32(blockSize),
			UpdateTime: time.Now().UnixMilli(),
		}
	}()

	o, ok := opt.(CipherSuite)
	if !ok {
		codec.aead = plainAEAD{castagnoliCrcTable}
		codec.spec = nil
		return
	}
	suite := o.CipherSuite()
	switch suite {
	case "", "plain":
		codec.aead = plainAEAD{castagnoliCrcTable}
		codec.spec = nil
		return
	case "crc32":
		codec.aead = crc32AEAD{castagnoliCrcTable}
		codec.spec = []byte{}
		return
	case "aes-256-gcm":
		key := getCipherKey(opt)
		if len(key) != 32 {
			err = fmt.Errorf("aes-256-gcm: %w size", ErrInvalidCipherKey)
			return
		}

		codec.aead, err = aesGCMAEAD(key)
		if err != nil {
			err = fmt.Errorf("aes-256-gcm: %w", err)
			return
		}

		codec.spec = binary.AppendVarint(nil, aes_256_gcm)
		return
	}
	err = fmt.Errorf("%w: %s", ErrInvalidCipherSuite, suite)
	return
}

func (codec *codec) size() int { // at least 4
	if codec.aead == nil {
		return 0
	}

	return codec.aead.NonceSize() + codec.aead.Overhead()
}

func (codec *codec) decode(buffer []byte, blockID BlockID) (err error) {
	// if codec.aead == nil {
	// 	return ErrClosed
	// }

	off := len(buffer) - codec.aead.NonceSize()
	var ad [4]byte
	binary.LittleEndian.PutUint32(ad[:], blockID)
	_, err = codec.aead.Open(buffer[:0], buffer[off:], buffer[:off], ad[:])
	return
}

func (codec *codec) encode(buffer []byte, blockID BlockID) {
	// if codec.aead == nil {
	// 	return
	// }

	off := len(buffer) - codec.aead.NonceSize()
	rand.Read(buffer[off:])
	var ad [4]byte
	binary.LittleEndian.PutUint32(ad[:], blockID)
	codec.aead.Seal(buffer[:0], buffer[off:], buffer[:off-codec.aead.Overhead()], ad[:])
}

var castagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)

type plainAEAD struct{ table *crc32.Table }

func (c plainAEAD) NonceSize() int { return 0 }
func (c plainAEAD) Overhead() int  { return 4 }

func (c plainAEAD) Seal(dst, nonce, plaintext, additionalData []byte) []byte {
	sum := crc32.Checksum(plaintext, c.table)
	binary.LittleEndian.AppendUint32(plaintext, sum)
	return nil
}

func (c plainAEAD) Open(dst, nonce, ciphertext, additionalData []byte) ([]byte, error) {
	off := len(ciphertext) - 4
	chksum := binary.LittleEndian.Uint32(ciphertext[off:])
	sum := crc32.Checksum(ciphertext[:off], c.table)
	if sum == chksum {
		return nil, nil
	}
	return nil, ErrInvalidChecksum
}

type crc32AEAD struct{ table *crc32.Table }

func (c crc32AEAD) NonceSize() int { return 0 }
func (c crc32AEAD) Overhead() int  { return 4 }

func (c crc32AEAD) Seal(dst, nonce, plaintext, additionalData []byte) []byte {
	sum := crc32.Checksum(plaintext, c.table)
	sum = crc32.Update(sum, c.table, additionalData)
	binary.LittleEndian.AppendUint32(plaintext, sum)
	return nil
}

func (c crc32AEAD) Open(dst, nonce, ciphertext, additionalData []byte) ([]byte, error) {
	off := len(ciphertext) - 4
	chksum := binary.LittleEndian.Uint32(ciphertext[off:])
	sum := crc32.Checksum(ciphertext[:off], c.table)
	sum = crc32.Update(sum, c.table, additionalData)
	if sum == chksum {
		return nil, nil
	}
	return nil, ErrInvalidChecksum
}

func aesGCMAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
