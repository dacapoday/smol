package heap

import (
	"testing"
)

func TestRingEmpty(t *testing.T) {
	var r ring
	r.capacity = 7
	r.reset()

	if !r.empty() {
		t.Error("new ring should be empty")
	}
	if r.full() {
		t.Error("new ring should not be full")
	}

	id := r.shift()
	if id != 0 {
		t.Errorf("shift from empty ring should return 0, got %d", id)
	}
}

func TestRingPushShift(t *testing.T) {
	var r ring
	r.capacity = 7
	r.reset()

	ids := []BlockID{11, 12, 13, 14, 15, 16, 17}

	for _, id := range ids {
		if !r.push(id) {
			t.Fatalf("failed to push %d", id)
		}
	}

	if r.empty() {
		t.Error("ring should not be empty")
	}
	if !r.full() {
		t.Error("ring should be full")
	}

	if r.push(18) {
		t.Error("push to full ring should fail")
	}

	for _, expected := range ids {
		got := r.shift()
		if got != expected {
			t.Errorf("expected %d, got %d", expected, got)
		}
	}

	if !r.empty() {
		t.Error("ring should be empty after all shifts")
	}

	got := r.shift()
	if got != 0 {
		t.Errorf("shift from empty ring should return 0, got %d", got)
	}
}

func TestRingWrapAround(t *testing.T) {
	var r ring
	r.capacity = 7
	r.reset()

	ids := []BlockID{11, 12, 13, 14, 15, 16, 17}

	for _, id := range ids {
		r.push(id)
	}

	for _, expected := range ids {
		got := r.shift()
		if got != expected {
			t.Errorf("expected %d, got %d", expected, got)
		}
	}

	for _, id := range ids {
		if !r.push(id) {
			t.Fatalf("failed to push %d on second round", id)
		}
	}

	for _, expected := range ids {
		got := r.shift()
		if got != expected {
			t.Errorf("second round: expected %d, got %d", expected, got)
		}
	}
}

func TestRingUnshift(t *testing.T) {
	var r ring
	r.capacity = 7
	r.reset()

	ids := []BlockID{11, 12, 13, 14, 15, 16, 17}

	for _, id := range ids {
		if !r.unshift(id) {
			t.Fatalf("failed to unshift %d", id)
		}
	}

	if !r.full() {
		t.Error("ring should be full")
	}

	if r.unshift(18) {
		t.Error("unshift to full ring should fail")
	}

	for i := len(ids) - 1; i >= 0; i-- {
		expected := ids[i]
		got := r.shift()
		if got != expected {
			t.Errorf("expected %d, got %d", expected, got)
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
