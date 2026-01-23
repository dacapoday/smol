package heap

import (
	"bytes"
	"errors"
	"testing"

	"github.com/dacapoday/smol/mem"
)

func TestBlockLoad(t *testing.T) {
	var b block[*mem.File]
	b.load(new(mem.File), 4096, 2)

	if b.size != 4096 || b.count != 2 || b.limit != 2 {
		t.Errorf("got size=%d count=%d limit=%d", b.size, b.count, b.limit)
	}
}

func TestBlockClose(t *testing.T) {
	var b block[*mem.File]
	b.load(new(mem.File), 4096, 2)

	if err := b.close(); err != nil {
		t.Fatal(err)
	}
	if b.size != 0 || b.count != 0 || b.limit != 0 {
		t.Errorf("not zeroed: size=%d count=%d limit=%d", b.size, b.count, b.limit)
	}
}

func TestBlockReadWrite(t *testing.T) {
	file := new(mem.File)
	var b block[*mem.File]
	b.load(file, 512, 2)
	file.Truncate(512 * 3)

	data := []byte("hello block")
	if _, err := b.writeAt(data, 2); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, len(data))
	if _, err := b.readAt(buf, 2); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, data) {
		t.Errorf("got %q, want %q", buf, data)
	}
}

func TestBlockExtend(t *testing.T) {
	var b block[*mem.File]
	b.load(new(mem.File), 512, 2)

	id, err := b.extend()
	if err != nil {
		t.Fatal(err)
	}
	if id != 2 || b.count != 3 {
		t.Errorf("got id=%d count=%d", id, b.count)
	}
	if b.limit <= 2 {
		t.Errorf("grow not triggered: limit=%d", b.limit)
	}
}

func TestBlockExtendOverflow(t *testing.T) {
	var b block[*mem.File]
	b.load(new(mem.File), 1, 1<<32-1)
	b.limit = 1<<32 - 1 // MaxUint32

	// 即将绕回：count = MaxUint32
	id, err := b.extend()
	if err != nil || id != 1<<32-1 {
		t.Fatalf("before overflow: id=%d err=%v", id, err)
	}
	if b.count != 0 {
		t.Fatalf("count should wrap to 0: got %d", b.count)
	}

	// 绕回后：count = 0
	id, err = b.extend()
	if !errors.Is(err, ErrNoSpace) || id != 0 {
		t.Errorf("after overflow: id=%d err=%v", id, err)
	}
}

func TestBlockGrow(t *testing.T) {
	var b block[*mem.File]
	b.load(new(mem.File), 4096, 10)

	if err := b.grow(100); err != nil {
		t.Fatal(err)
	}
	if b.limit != 110 {
		t.Errorf("limit: got %d, want 110", b.limit)
	}
}

func TestBlockGrowMax(t *testing.T) {
	var b block[*mem.File]
	b.load(new(mem.File), 1, 100)
	b.limit = 1<<32 - 100

	if err := b.grow(200); err != nil {
		t.Fatal(err)
	}
	if b.limit != 1<<32-1 {
		t.Errorf("limit: got %d, want MaxUint32", b.limit)
	}
}
