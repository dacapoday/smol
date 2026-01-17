package heap

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"
)

func TestMetaRoundTrip(t *testing.T) {
	m := Meta{
		Ckp:          789,
		UpdateTime:   1234567890,
		BlockSize:    4096,
		BlockCount:   100,
		ID:           123,
		PrevID:       456,
		FreeRecycled: 10,
		FreeTotal:    50,
		EntrySize:    15,
		EntryID:      7,
		Entry:        []byte("test entry"),
		Freelist:     []byte{1, 2, 3, 4},
		CodecSpec:    []byte{0x02},
	}

	var buf bytes.Buffer
	if err := encodeMeta(&buf, &m); err != nil {
		t.Fatal(err)
	}

	var got Meta
	if err := decodeMeta(&buf, &got); err != nil {
		t.Fatal(err)
	}

	if got.Ckp != m.Ckp || got.BlockSize != m.BlockSize || got.BlockCount != m.BlockCount {
		t.Errorf("basic fields mismatch")
	}
	if got.ID != m.ID || got.PrevID != m.PrevID {
		t.Errorf("ID fields mismatch")
	}
	if got.FreeRecycled != m.FreeRecycled || got.FreeTotal != m.FreeTotal {
		t.Errorf("free fields mismatch")
	}
	if !bytes.Equal(got.Entry, m.Entry) || !bytes.Equal(got.Freelist, m.Freelist) {
		t.Errorf("bytes fields mismatch")
	}
}

func TestMetaEmpty(t *testing.T) {
	var m Meta
	var buf bytes.Buffer

	if err := encodeMeta(&buf, &m); err != nil {
		t.Fatal(err)
	}

	var got Meta
	if err := decodeMeta(&buf, &got); err != nil {
		t.Fatal(err)
	}

	if got.Ckp != 0 || got.BlockSize != 0 || got.BlockCount != 0 {
		t.Error("zero values should decode as zero")
	}
}

func TestMetaMaxValues(t *testing.T) {
	m := Meta{
		Ckp:          math.MaxUint32,
		UpdateTime:   math.MaxInt64,
		BlockSize:    math.MaxUint32,
		BlockCount:   math.MaxUint32,
		ID:           math.MaxUint32,
		PrevID:       math.MaxUint32,
		FreeRecycled: math.MaxUint32,
		FreeTotal:    math.MaxUint32,
		EntrySize:    math.MaxUint32,
		EntryID:      math.MaxUint32,
	}

	var buf bytes.Buffer
	if err := encodeMeta(&buf, &m); err != nil {
		t.Fatal(err)
	}

	var got Meta
	if err := decodeMeta(&buf, &got); err != nil {
		t.Fatal(err)
	}

	if got.Ckp != m.Ckp || got.UpdateTime != m.UpdateTime {
		t.Error("max values mismatch")
	}
}

func TestMetaNilVsEmpty(t *testing.T) {
	// nil 不写入
	m1 := Meta{Ckp: 1, Entry: nil}
	var buf1 bytes.Buffer
	encodeMeta(&buf1, &m1)

	// 空切片写入（长度 0）
	m2 := Meta{Ckp: 1, Entry: []byte{}}
	var buf2 bytes.Buffer
	encodeMeta(&buf2, &m2)

	if buf1.Len() >= buf2.Len() {
		t.Errorf("nil should produce smaller output: nil=%d empty=%d", buf1.Len(), buf2.Len())
	}

	var got Meta
	decodeMeta(&buf2, &got)
	if got.Entry == nil {
		t.Error("empty slice should decode as non-nil")
	}
}

func TestMetaInvalidChecksum(t *testing.T) {
	m := Meta{Ckp: 1, BlockSize: 4096}
	var buf bytes.Buffer
	encodeMeta(&buf, &m)

	// 篡改最后一个字节（CRC）
	data := buf.Bytes()
	data[len(data)-1] ^= 0xFF

	var got Meta
	err := decodeMeta(bytes.NewReader(data), &got)
	if !errors.Is(err, ErrInvalidMeta) {
		t.Errorf("expected ErrInvalidMeta, got %v", err)
	}
}

func TestMetaTruncated(t *testing.T) {
	m := Meta{Ckp: 1, BlockSize: 4096, Entry: []byte("long entry data")}
	var buf bytes.Buffer
	encodeMeta(&buf, &m)

	// 截断到 Entry 字段中间（key + length 已写，但 data 不完整）
	data := buf.Bytes()[:10]

	var got Meta
	err := decodeMeta(bytes.NewReader(data), &got)
	if err == nil {
		t.Error("expected error for truncated data")
	}
}

func TestMetaBufferOverflow(t *testing.T) {
	m := Meta{Entry: make([]byte, 100)}
	buf := make(Buffer, 0, 16) // 容量太小

	err := encodeMeta(&buf, &m)
	if !errors.Is(err, ErrOutOfRange) {
		t.Errorf("expected ErrOutOfRange, got %v", err)
	}
}

func TestSizeMeta(t *testing.T) {
	m := Meta{
		Ckp:          789,
		UpdateTime:   1234567890,
		BlockSize:    4096,
		BlockCount:   100,
		ID:           123,
		PrevID:       456,
		FreeRecycled: 10,
		FreeTotal:    50,
		EntrySize:    15,
		EntryID:      7,
		Entry:        []byte("test entry"),
		Freelist:     []byte{1, 2, 3, 4},
		CodecSpec:    []byte{0x02},
	}

	var buf bytes.Buffer
	encodeMeta(&buf, &m)

	if got := sizeMeta(&m); got != buf.Len() {
		t.Errorf("sizeMeta=%d, actual=%d", got, buf.Len())
	}
}

func TestSizeMetaEmpty(t *testing.T) {
	var m Meta
	var buf bytes.Buffer
	encodeMeta(&buf, &m)

	if got := sizeMeta(&m); got != buf.Len() {
		t.Errorf("sizeMeta=%d, actual=%d", got, buf.Len())
	}
}

func TestSizeUvarint(t *testing.T) {
	var buf [binary.MaxVarintLen64]byte
	cases := []uint64{
		0, 1, 127, 128, 16383, 16384,
		1<<21 - 1, 1 << 21, 1<<28 - 1, 1 << 28,
		1<<35 - 1, 1 << 35, 1<<42 - 1, 1 << 42,
		1<<49 - 1, 1 << 49, 1<<56 - 1, 1 << 56,
		1<<63 - 1, 1 << 63, math.MaxUint64,
	}
	for _, v := range cases {
		want := binary.PutUvarint(buf[:], v)
		if got := sizeUvarint(v); got != want {
			t.Errorf("sizeUvarint(%d)=%d, want %d", v, got, want)
		}
	}
}

func TestSizeVarint(t *testing.T) {
	var buf [binary.MaxVarintLen64]byte
	cases := []int64{
		0, 1, -1, 63, -64, 64, -65,
		1<<20 - 1, -1 << 20, 1<<27 - 1, -1 << 27,
		math.MaxInt64, math.MinInt64,
	}
	for _, v := range cases {
		want := binary.PutVarint(buf[:], v)
		if got := sizeVarint(v); got != want {
			t.Errorf("sizeVarint(%d)=%d, want %d", v, got, want)
		}
	}
}
