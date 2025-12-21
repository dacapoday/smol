package iterator

// Combine extends Merge to filter out entries with nil values.
//
// It embeds Merge and overrides iteration methods to skip entries where the 'over'
// iterator returns nil values (tombstones). When a key exists in both iterators
// but 'over' has nil value, the entry is skipped entirely rather than falling back
// to 'base'.
//
// Other methods (Valid, Error, Key, Val, Over, Base, Cover) are inherited from Merge.
//
// This is useful for implementing LSM tree compaction where deletions are marked
// with nil values in the overlay.
type Combine[Over Iterator, Base Iterator] struct {
	merge[Over, Base]
}

type merge[Over Iterator, Base Iterator] = Merge[Over, Base]

// Load initializes the combine iterator with the given iterators.
// If idle is provided, copies its state; otherwise initializes to default state.
func (iter *Combine[Over, Base]) Load(over Over, base Base, idle *Combine[Over, Base]) {
	if idle == nil {
		iter.load(over, base)
	} else {
		iter.copy(over, base, &idle.merge)
	}
}

var _ Iterator = (*Combine[Iterator, Iterator])(nil)

// Next advances to the next key.
func (iter *Combine[Over, Base]) Next() bool {
	for {
		if !iter.merge.Next() {
			return false
		}
		if iter.merge.cover && iter.merge.over.Val() == nil {
			continue
		}
		return true
	}
}

// Prev moves to the previous key.
func (iter *Combine[Over, Base]) Prev() bool {
	for {
		if !iter.merge.Prev() {
			return false
		}
		if iter.merge.cover && iter.merge.over.Val() == nil {
			continue
		}
		return true
	}
}

// SeekFirst positions at the first key.
func (iter *Combine[Over, Base]) SeekFirst() bool {
	if !iter.merge.SeekFirst() {
		return false
	}
	if iter.merge.cover && iter.merge.over.Val() == nil {
		return iter.Next()
	}
	return true
}

// SeekLast positions at the last key.
func (iter *Combine[Over, Base]) SeekLast() bool {
	if !iter.merge.SeekLast() {
		return false
	}
	if iter.merge.cover && iter.merge.over.Val() == nil {
		return iter.Prev()
	}
	return true
}

// Seek positions the iterator at the first key >= the given key.
func (iter *Combine[Over, Base]) Seek(key []byte) bool {
	if !iter.merge.Seek(key) {
		return false
	}
	if iter.merge.cover && iter.merge.over.Val() == nil {
		return iter.Next()
	}
	return true
}
