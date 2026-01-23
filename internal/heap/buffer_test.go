package heap

import (
	"bytes"
	"testing"
)

func TestBufferWrite(t *testing.T) {
	buf := make(Buffer, 0, 10)

	n, err := buf.Write([]byte("hello"))
	if err != nil || n != 5 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if !bytes.Equal(buf, []byte("hello")) {
		t.Errorf("got %q", buf)
	}

	n, err = buf.Write([]byte("world"))
	if err != nil || n != 5 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if !bytes.Equal(buf, []byte("helloworld")) {
		t.Errorf("got %q", buf)
	}
}

func TestBufferOverflow(t *testing.T) {
	buf := make(Buffer, 0, 5)
	buf.Write([]byte("hello"))

	n, err := buf.Write([]byte("x"))
	if err != errOutOfRange || n != 0 {
		t.Errorf("n=%d err=%v", n, err)
	}
	if !bytes.Equal(buf, []byte("hello")) {
		t.Error("buffer should be unchanged")
	}
}

func TestBufferZeroCapacity(t *testing.T) {
	buf := make(Buffer, 0, 0)

	n, err := buf.Write([]byte("x"))
	if err != errOutOfRange || n != 0 {
		t.Errorf("n=%d err=%v", n, err)
	}
}

func TestBufferEmptyWrite(t *testing.T) {
	buf := make(Buffer, 0, 10)

	n, err := buf.Write(nil)
	if err != nil || n != 0 || len(buf) != 0 {
		t.Errorf("n=%d err=%v len=%d", n, err, len(buf))
	}
}
