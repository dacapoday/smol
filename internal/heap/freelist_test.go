package heap

import "testing"

func TestFreelistRoundTrip(t *testing.T) {
	caps := []uint16{1, 10, 100, 1000}

	for _, cap := range caps {
		var src, dst ring
		src.capacity = cap
		dst.capacity = cap
		src.reset()
		dst.reset()

		for i := uint16(0); i < cap; i++ {
			src.push(BlockID(i + 2))
		}

		freelist := make(Freelist, freelistSize(cap))
		ring2freelist(&src, 42, freelist)

		if freelist.invalid() {
			t.Fatalf("cap=%d: freelist should be valid", cap)
		}
		if freelist.Count() != cap {
			t.Errorf("cap=%d: count=%d", cap, freelist.Count())
		}
		if freelist.Prev() != 42 {
			t.Errorf("cap=%d: prev=%d", cap, freelist.Prev())
		}

		freelist2ring(freelist, &dst, freelist.Count())

		for i := uint16(0); i < cap; i++ {
			if src.shift() != dst.shift() {
				t.Errorf("cap=%d: mismatch at %d", cap, i)
				break
			}
		}
	}
}

func TestFreelistInvalid(t *testing.T) {
	freelist := make(Freelist, 100)

	// uninitialized
	if !freelist.invalid() {
		t.Error("uninitialized should be invalid")
	}

	// valid data
	var r ring
	r.capacity = 1
	r.reset()
	r.push(10)
	ring2freelist(&r, 0, freelist)

	if freelist.invalid() {
		t.Error("valid data should not be invalid")
	}

	// corrupt marker
	freelist[1] = 0
	if !freelist.invalid() {
		t.Error("corrupted marker should be invalid")
	}
}

func TestFreelistCapacity(t *testing.T) {
	// verify size/capacity roundtrip
	for _, cap := range []uint16{1, 100, 1000} {
		size := freelistSize(cap)
		gotCap := freelistCapacity(int64(size))
		if gotCap != cap {
			t.Errorf("cap=%d: size=%d -> gotCap=%d", cap, size, gotCap)
		}
	}

	// verify capacity doesn't exceed block size
	for _, size := range []int64{512, 4096, 65536} {
		cap := freelistCapacity(size)
		if int64(freelistSize(cap)) > size {
			t.Errorf("size=%d: cap=%d exceeds", size, cap)
		}
	}
}

func TestFreelistID(t *testing.T) {
	var r ring
	r.capacity = 3
	r.reset()
	r.push(10)
	r.push(20)
	r.push(30)

	freelist := make(Freelist, freelistSize(3))
	ring2freelist(&r, 0, freelist)

	// freelist stores newest first: ID(0)=30, ID(1)=20, ID(2)=10
	if freelist.ID(0) != 30 || freelist.ID(1) != 20 || freelist.ID(2) != 10 {
		t.Errorf("ID order: %d, %d, %d", freelist.ID(0), freelist.ID(1), freelist.ID(2))
	}
}
