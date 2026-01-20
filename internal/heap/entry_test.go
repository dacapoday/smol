package heap

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/dacapoday/smol/mem"
)

func testEntryRoundTrip(t *testing.T, opt testOption, entrySizes func(pageSize, blockSize int) []int) {
	t.Helper()

	file := new(mem.File)
	var heap Heap[*mem.File]
	_, ckpt, err := heap.Load(file, opt)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	ckpt.Release()

	pageSize := heap.PageSize()
	blockSize := heap.BlockSize()
	sizes := entrySizes(pageSize, blockSize)

	for _, size := range sizes {
		t.Run(entrySizeName(size, pageSize, blockSize), func(t *testing.T) {
			entry := make([]byte, size)
			rand.Read(entry)

			_, ckpt, err := heap.Commit(entry)
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
				t.Errorf("entry mismatch: got len=%d, want len=%d", len(meta.Entry), len(entry))
			}
			ckpt.Release()
			heap2.Close()

			file.ReadFrom(&backup)
			heap.Load(file, opt)
		})
	}
	heap.Close()
}

func entrySizeName(size, pageSize, blockSize int) string {
	switch size {
	case 0:
		return "empty"
	case pageSize / 2:
		return "halfPage"
	case pageSize - 2:
		return "pageMinus2"
	case pageSize - 5:
		return "pageMinus5"
	case pageSize:
		return "pageSize"
	case blockSize:
		return "blockSize"
	case (pageSize + blockSize) / 2:
		return "midPageBlock"
	}
	if size < 64 {
		return "inline"
	}
	return "custom"
}

func codecEntrySizes(pageSize, blockSize int) []int {
	return []int{
		16,           // inline
		pageSize / 2, // half pageSize
		pageSize - 5, // pageSize - 5
		pageSize - 2, // pageSize - 2
		pageSize,     // pageSize
	}
}

func plainEntrySizes(pageSize, blockSize int) []int {
	return []int{
		16,                         // inline
		pageSize / 2,               // half pageSize
		pageSize - 5,               // pageSize - 5
		pageSize - 2,               // pageSize - 2
		pageSize,                   // pageSize
		(pageSize + blockSize) / 2, // between pageSize and blockSize
		blockSize,                  // blockSize
	}
}

func TestEntryCRC32RoundTrip(t *testing.T) {
	opt := testOption{
		magicCode:         [4]byte{'c', 'r', 'c', '3'},
		retainCheckpoints: 3,
		cipherSuite:       "crc32",
	}
	testEntryRoundTrip(t, opt, codecEntrySizes)
}

func TestEntryAES256RoundTrip(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	opt := testOption{
		magicCode:         [4]byte{'a', 'e', 's', '2'},
		retainCheckpoints: 3,
		cipherSuite:       "aes-256-gcm",
		cipherKey:         key,
	}
	testEntryRoundTrip(t, opt, codecEntrySizes)
}

func TestEntryPlainRoundTrip(t *testing.T) {
	opt := testOption{
		magicCode:         [4]byte{'p', 'l', 'a', 'n'},
		retainCheckpoints: 3,
		cipherSuite:       "plain",
	}
	testEntryRoundTrip(t, opt, plainEntrySizes)
}
