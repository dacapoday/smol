package iterator

// Iterator represents a cursor over a sorted key-value dataset.
// The iterator maintains a current position and can be moved forward or backward
// through the dataset in sorted key order.
//
// Usage:
//
//	for iter.SeekFirst(); iter.Valid(); iter.Next() {
//	    key, val := iter.Key(), iter.Val()
//	    // process key, val
//	}
//	if err := iter.Error(); err != nil {
//	    // handle error
//	}
type Iterator interface {
	// Valid returns true if positioned at a valid key-value pair.
	// Returns false when not positioned; check Error() to distinguish the cause.
	Valid() bool

	// Error returns any error that occurred during operations.
	// Returns nil when not positioned due to normal conditions (initial state,
	// boundary reached, empty dataset). Returns non-nil for internal errors
	// (I/O failures, data corruption, etc.).
	Error() error

	// Key returns the key at the current iterator position.
	// The returned slice is valid only until the next iterator operation.
	// Behavior is undefined if Valid() returns false.
	Key() []byte

	// Val returns the value at the current iterator position.
	// Returns nil for tombstone entries (deleted keys in LSM-tree context).
	// The returned slice is valid only until the next iterator operation.
	// Behavior is undefined if Valid() returns false.
	Val() []byte

	// Next advances the iterator to the next key-value pair in ascending order.
	// Returns true if the advance was successful and the iterator is positioned
	// at a valid entry. Returns false if the iterator has reached the end or
	// encountered an error. Use Error() to distinguish between these cases.
	Next() bool

	// Prev moves the iterator to the previous key-value pair in descending order.
	// Returns true if the move was successful and the iterator is positioned
	// at a valid entry. Returns false if the iterator has reached the beginning
	// or encountered an error. Use Error() to distinguish between these cases.
	Prev() bool

	// SeekFirst positions the iterator at the first (smallest) key in the dataset.
	// Returns true if successful and the iterator is positioned at a valid entry.
	// Returns false if the dataset is empty or an error occurred.
	// Use Error() to distinguish between these cases.
	SeekFirst() bool

	// SeekLast positions the iterator at the last (largest) key in the dataset.
	// Returns true if successful and the iterator is positioned at a valid entry.
	// Returns false if the dataset is empty or an error occurred.
	// Use Error() to distinguish between these cases.
	SeekLast() bool

	// Seek positions the iterator at the first key that is greater than or equal
	// to the given key. Returns true if positioned at a valid entry,
	// false otherwise. Use Error() to check for errors.
	Seek(key []byte) bool
}
