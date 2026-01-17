package heap

import "testing"

func TestRingPushShift(t *testing.T) {
	var r ring
	r.capacity = 5
	r.reset()

	// push until full
	for i := BlockID(1); i <= 5; i++ {
		if !r.push(i) {
			t.Fatalf("push %d failed", i)
		}
	}
	if !r.full() || r.push(6) {
		t.Error("should be full")
	}

	// shift all, verify FIFO
	for i := BlockID(1); i <= 5; i++ {
		if got := r.shift(); got != i {
			t.Errorf("shift: got %d, want %d", got, i)
		}
	}
	if !r.empty() || r.shift() != 0 {
		t.Error("should be empty")
	}

	// wrap around: push again after shift
	for i := BlockID(10); i <= 14; i++ {
		r.push(i)
	}
	for i := BlockID(10); i <= 14; i++ {
		if got := r.shift(); got != i {
			t.Errorf("wrap: got %d, want %d", got, i)
		}
	}
}

func TestRingUnshift(t *testing.T) {
	var r ring
	r.capacity = 5
	r.reset()

	// unshift reverses order
	for i := BlockID(1); i <= 5; i++ {
		if !r.unshift(i) {
			t.Fatalf("unshift %d failed", i)
		}
	}
	if !r.full() || r.unshift(6) {
		t.Error("should be full")
	}

	// shift returns reverse order: 5, 4, 3, 2, 1
	for i := BlockID(5); i >= 1; i-- {
		if got := r.shift(); got != i {
			t.Errorf("got %d, want %d", got, i)
		}
	}
}

func TestRingTopBottom(t *testing.T) {
	var r ring
	r.capacity = 5
	r.reset()

	// empty
	if r.top() != 0 || r.bottom() != 0 {
		t.Error("empty ring: top/bottom should be 0")
	}

	r.push(10)
	r.push(20)
	if r.top() != 10 || r.bottom() != 20 {
		t.Errorf("top=%d bottom=%d, want 10, 20", r.top(), r.bottom())
	}

	r.shift()
	if r.top() != 20 || r.bottom() != 20 {
		t.Errorf("after shift: top=%d bottom=%d, want both 20", r.top(), r.bottom())
	}

	// unshift inserts at head
	r.unshift(5)
	if r.top() != 5 || r.bottom() != 20 {
		t.Errorf("after unshift: top=%d bottom=%d, want 5, 20", r.top(), r.bottom())
	}
}

func TestRingZeroCapacity(t *testing.T) {
	var r ring
	// zero capacity: both empty and full
	if !r.empty() || !r.full() {
		t.Error("zero cap should be empty and full")
	}
	if r.push(10) || r.unshift(10) {
		t.Error("push/unshift should fail")
	}
}

func TestRingFreelist(t *testing.T) {
	var r ring
	r.capacity = 5
	r.reset()

	// create wrap-around state
	for i := BlockID(1); i <= 5; i++ {
		r.push(i)
	}
	r.shift() // remove 1
	r.shift() // remove 2
	r.push(6)
	r.push(7)
	// buffer: [6, 7, 3, 4, 5], head=2, FIFO: 3, 4, 5, 6, 7

	expected := []BlockID{3, 4, 5, 6, 7}
	idx := 0
	for _, id := range r.freelist {
		if BlockID(id) != expected[idx] {
			t.Errorf("freelist[%d]: got %d, want %d", idx, id, expected[idx])
		}
		idx++
	}
	if idx != len(expected) {
		t.Errorf("freelist count: got %d, want %d", idx, len(expected))
	}
}

func TestRingReset(t *testing.T) {
	var r ring
	r.capacity = 5
	r.reset()

	r.push(10)
	r.push(20)
	r.shift()

	r.reset()
	if !r.empty() || r.head != 0 || r.tail != 0 {
		t.Errorf("after reset: empty=%v head=%d tail=%d", r.empty(), r.head, r.tail)
	}

	// usable after reset
	r.push(100)
	if r.shift() != 100 {
		t.Error("not usable after reset")
	}
}
