package heap

import "testing"

func TestQueueEmpty(t *testing.T) {
	var q queue
	if q.shift() != 0 || q.top() != 0 || q.bottom() != 0 {
		t.Error("empty queue: all should return 0")
	}
}

func TestQueuePushShift(t *testing.T) {
	var q queue

	for i := BlockID(1); i <= 7; i++ {
		q.push(i)
	}

	for i := BlockID(1); i <= 7; i++ {
		if got := q.shift(); got != i {
			t.Errorf("got %d, want %d", got, i)
		}
	}
	if q.shift() != 0 {
		t.Error("should be empty")
	}
}

func TestQueueUnshift(t *testing.T) {
	var q queue

	// unshift reverses order
	for i := BlockID(1); i <= 5; i++ {
		q.unshift(i)
	}

	// shift returns: 5, 4, 3, 2, 1
	for i := BlockID(5); i >= 1; i-- {
		if got := q.shift(); got != i {
			t.Errorf("got %d, want %d", got, i)
		}
	}
}

func TestQueueTopBottom(t *testing.T) {
	var q queue

	q.push(10)
	q.push(20)
	q.push(30)

	if q.top() != 10 || q.bottom() != 30 {
		t.Errorf("top=%d bottom=%d, want 10, 30", q.top(), q.bottom())
	}

	q.shift()
	if q.top() != 20 || q.bottom() != 30 {
		t.Errorf("after shift: top=%d bottom=%d, want 20, 30", q.top(), q.bottom())
	}

	q.unshift(5)
	if q.top() != 5 || q.bottom() != 30 {
		t.Errorf("after unshift: top=%d bottom=%d, want 5, 30", q.top(), q.bottom())
	}
}

func TestQueueExpansion(t *testing.T) {
	var q queue

	// push 3000 elements triggers multiple node expansions
	// 4 → 8 → 16 → 32 → ... → 1024 (capped)
	n := 3000
	for i := 1; i <= n; i++ {
		q.push(BlockID(i))
	}

	for i := 1; i <= n; i++ {
		if got := q.shift(); got != BlockID(i) {
			t.Errorf("got %d, want %d", got, i)
		}
	}

	// extra shift triggers head cleanup
	if q.shift() != 0 || q.head != nil {
		t.Error("should be empty with nil head")
	}
}

func TestQueueTopAcrossNodes(t *testing.T) {
	var q queue

	// push 5 elements (capacity 4), triggers new node
	for i := 1; i <= 5; i++ {
		q.push(BlockID(i))
	}

	// shift 4 to empty first node's ring
	for i := 1; i <= 4; i++ {
		q.shift()
	}

	// head.ring is empty, but head.next has element 5
	if q.top() != 5 {
		t.Errorf("top across nodes: got %d, want 5", q.top())
	}
}
