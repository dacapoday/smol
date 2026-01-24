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
	head, overflowSize, overflowID, err := Write(&b, data, 10)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// collect all pages via Iter
	var collected []byte
	collected = append(collected, head...)
	for chunk, err := range Iter(&b, overflowSize, overflowID) {
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
	head, overflowSize, overflowID, err := Write(&b, data, 5)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	var collected []byte
	collected = append(collected, head...)
	for chunk, err := range Iter(&b, overflowSize, overflowID) {
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
	_, overflowSize, overflowID, err := Write(&b, data, 10)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// stop after first page
	count := 0
	for _, err := range Iter(&b, overflowSize, overflowID) {
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

	// overflowSize=0 with invalid ID should yield nothing
	count := 0
	for _, err := range Iter(&b, 0, 0) {
		if err != nil {
			t.Fatalf("Iter error: %v", err)
		}
		count++
	}

	if count != 0 {
		t.Errorf("Expected 0 iterations for zero size, got %d", count)
	}
}

func TestCompare(t *testing.T) {
	var f mem.File
	var b block.Heap[*mem.File]

	_, ckpt, err := b.Load(&f, option{})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	defer ckpt.Release()
	defer b.Close()

	// data with overflow: head="abc", overflow="defgh"
	data := []byte("abcdefgh")
	head, overflowSize, overflowID, err := Write(&b, data, 3)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if overflowID < 2 {
		t.Fatalf("expected overflow, got overflowID=%d", overflowID)
	}
	if overflowSize != 5 {
		t.Fatalf("expected overflowSize=5, got %d", overflowSize)
	}

	tests := []struct {
		key  string
		want int
	}{
		{"abcdefgh", 0},  // equal
		{"abcdefgi", 1},  // key > data
		{"abcdefgg", -1}, // key < data
		{"abcdefg", -1},  // key shorter (prefix)
		{"abcdefghi", 1}, // key longer
		{"abc", -1},      // key = head only
		{"ab", -1},       // key shorter than head
		{"abd", 1},       // key > head prefix
		{"abb", -1},      // key < head prefix
		{"abcd", -1},     // key ends in overflow
		{"abcdf", 1},     // key diverges in overflow
	}

	for _, tt := range tests {
		cmp, err := Compare(&b, []byte(tt.key), head, overflowSize, overflowID)
		if err != nil {
			t.Errorf("Compare(%q): error %v", tt.key, err)
			continue
		}
		if cmp != tt.want {
			t.Errorf("Compare(%q): got %d, want %d", tt.key, cmp, tt.want)
		}
	}
}

func TestCompareNoOverflow(t *testing.T) {
	var f mem.File
	var b block.Heap[*mem.File]

	_, ckpt, err := b.Load(&f, option{})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	defer ckpt.Release()
	defer b.Close()

	head := []byte("abc")

	tests := []struct {
		key  string
		want int
	}{
		{"abc", 0},
		{"abd", 1},
		{"abb", -1},
		{"ab", -1},
		{"abcd", 1},
	}

	for _, tt := range tests {
		cmp, err := Compare(&b, []byte(tt.key), head, 0, 0)
		if err != nil {
			t.Errorf("Compare(%q): error %v", tt.key, err)
			continue
		}
		if cmp != tt.want {
			t.Errorf("Compare(%q): got %d, want %d", tt.key, cmp, tt.want)
		}
	}
}

func TestCompareMultiPage(t *testing.T) {
	var f mem.File
	var b block.Heap[*mem.File]

	_, ckpt, err := b.Load(&f, option{})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	defer ckpt.Release()
	defer b.Close()

	// large data spanning multiple pages
	data := bytes.Repeat([]byte("abcdefghij"), 200) // 2000 bytes
	head, overflowSize, overflowID, err := Write(&b, data, 10)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if overflowID < 2 {
		t.Fatalf("expected overflow, got overflowID=%d", overflowID)
	}

	// count pages
	pageCount := 0
	for _, iterErr := range Iter(&b, overflowSize, overflowID) {
		if iterErr != nil {
			t.Fatalf("Iter error: %v", iterErr)
		}
		pageCount++
	}
	if pageCount < 2 {
		t.Fatalf("expected multi-page overflow, got %d pages", pageCount)
	}

	tests := []struct {
		name string
		key  []byte
		want int
	}{
		{"equal", data, 0},
		{"shorter prefix", data[:len(data)-1], -1},
		{"longer", append(data[:len(data):len(data)], 'x'), 1},
		{"differ at end", append(data[:len(data)-1:len(data)-1], data[len(data)-1]+1), 1},
		{"differ in middle", func() []byte {
			k := make([]byte, len(data))
			copy(k, data)
			k[1000] = k[1000] + 1 // modify byte in overflow region
			return k
		}(), 1},
		{"shorter in second page", data[:600], -1},
	}

	for _, tt := range tests {
		cmp, err := Compare(&b, tt.key, head, overflowSize, overflowID)
		if err != nil {
			t.Errorf("Compare(%s): error %v", tt.name, err)
			continue
		}
		if cmp != tt.want {
			t.Errorf("Compare(%s): got %d, want %d", tt.name, cmp, tt.want)
		}
	}
}

func TestCompareEdgeCases(t *testing.T) {
	var f mem.File
	var b block.Heap[*mem.File]

	_, ckpt, err := b.Load(&f, option{})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	defer ckpt.Release()
	defer b.Close()

	tests := []struct {
		name         string
		key          []byte
		head         []byte
		overflowSize int
		overflowID   uint32
		want         int
	}{
		{"empty key vs empty head", nil, nil, 0, 0, 0},
		{"empty key vs non-empty head", nil, []byte("a"), 0, 0, -1},
		{"non-empty key vs empty head", []byte("a"), nil, 0, 0, 1},
	}

	for _, tt := range tests {
		cmp, err := Compare(&b, tt.key, tt.head, tt.overflowSize, tt.overflowID)
		if err != nil {
			t.Errorf("Compare(%s): error %v", tt.name, err)
			continue
		}
		if cmp != tt.want {
			t.Errorf("Compare(%s): got %d, want %d", tt.name, cmp, tt.want)
		}
	}
}
