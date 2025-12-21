package iterator

import "bytes"

// Merge merges two sorted iterators into a single sorted iterator.
// Similar to merge iterators in LSM trees (e.g., LevelDB).
//
// The 'over' iterator acts as an overlay, taking precedence when both iterators
// have the same key. This is useful for implementing layered data structures where
// newer values override older ones.
/*
Merge State List
valid{
	only{
		next{
			cover{
				S1,
				S2
			},
			cover{
				S3,
				S4
			}
		},
		same{
			S5,
			next{
				cover{
					S6,
					S7
				},
				cover{
					S8,
					S9
				}
			}
		}
	},
	fault{
		cover{
			S10,
			S11
		},
		S12
	}
}
*/
type Merge[Over Iterator, Base Iterator] struct {
	over         Over
	base         Base
	valid, fault bool
	only, same   bool
	next, cover  bool
}

// Load initializes the merge iterator with the given iterators.
// If idle is provided, copies its state; otherwise initializes to default state.
func (iter *Merge[Over, Base]) Load(over Over, base Base, idle *Merge[Over, Base]) {
	if idle == nil {
		iter.load(over, base)
	} else {
		iter.copy(over, base, idle)
	}
}

func (iter *Merge[Over, Base]) load(over Over, base Base) {
	iter.over, iter.base = over, base
	iter.valid, iter.fault = false, false
	iter.only, iter.same = false, false
	iter.next, iter.cover = false, false
}

func (iter *Merge[Over, Base]) copy(over Over, base Base, idle *Merge[Over, Base]) {
	iter.over, iter.base = over, base
	iter.valid, iter.fault = idle.valid, idle.fault
	iter.only, iter.same = idle.only, idle.same
	iter.next, iter.cover = idle.next, idle.cover
}

// Over returns the overlay iterator.
func (iter *Merge[Over, Base]) Over() Over {
	return iter.over
}

// Base returns the base iterator.
func (iter *Merge[Over, Base]) Base() Base {
	return iter.base
}

// Cover returns true if the current key-value pair comes from the overlay iterator,
// false if it comes from the base iterator.
func (iter *Merge[Over, Base]) Cover() bool {
	return iter.cover
}

var _ Iterator = (*Merge[Iterator, Iterator])(nil)

// Valid returns true if the iterator points to a valid key-value pair.
func (iter *Merge[Over, Base]) Valid() bool {
	return iter.valid
}

// Error returns the first error encountered from either child iterator, or nil.
func (iter *Merge[Over, Base]) Error() error {
	if !iter.fault {
		return nil
	}
	if iter.cover {
		return iter.over.Error()
	}
	return iter.base.Error()
}

// Key returns the current key from the active child iterator.
func (iter *Merge[Over, Base]) Key() []byte {
	if !iter.valid {
		//deny S10,S11,S12
		return nil
	}
	if iter.cover {
		return iter.over.Key()
	}
	return iter.base.Key()
}

// Val returns the current value from the active child iterator.
func (iter *Merge[Over, Base]) Val() []byte {
	if !iter.valid {
		//deny S10,S11,S12
		return nil
	}
	if iter.cover {
		return iter.over.Val()
	}
	return iter.base.Val()
}

// Next advances to the next key.
func (iter *Merge[Over, Base]) Next() bool {
	if !iter.valid {
		//deny S10,S11,S12
		return false
	}
	var over, base bool
	if iter.only {
		if iter.next {
			if iter.cover {
				//is S1
				over = iter.over.Next()
				// base = false
			} else {
				//is S2
				base = iter.base.Next()
				// over = false
			}
		} else if iter.cover {
			//is S3
			base = iter.base.SeekFirst()
			over = iter.over.Next()
		} else {
			//is S4
			over = iter.over.SeekFirst()
			base = iter.base.Next()
		}
	} else if iter.same {
		//is S5
		over = iter.over.Next()
		base = iter.base.Next()
	} else if iter.next {
		if iter.cover {
			//is S6
			over = iter.over.Next()
			base = true
		} else {
			//is S7
			base = iter.base.Next()
			over = true
		}
	} else {
		//is S8,S9
		over = iter.over.Next()
		base = iter.base.Next()
	}
	return iter.mergeNext(over, base)
}

// Prev moves to the previous key.
func (iter *Merge[Over, Base]) Prev() bool {
	if !iter.valid {
		//deny S10,S11,S12
		return false
	}
	var over, base bool
	if iter.only {
		if iter.next {
			if iter.cover {
				//is S1
				base = iter.base.SeekLast()
				over = iter.over.Prev()
			} else {
				//is S2
				over = iter.over.SeekLast()
				base = iter.base.Prev()
			}
		} else if iter.cover {
			//is S3
			over = iter.over.Prev()
			// base = false
		} else {
			//is S4
			base = iter.base.Prev()
			// over = false
		}
	} else if iter.same {
		//is S5
		over = iter.over.Prev()
		base = iter.base.Prev()
	} else if iter.next {
		//is S6,S7
		base = iter.base.Prev()
		over = iter.over.Prev()
	} else if iter.cover {
		//is S8
		over = iter.over.Prev()
		base = true
	} else {
		//is S9
		base = iter.base.Prev()
		over = true
	}
	return iter.mergePrev(over, base)
}

// SeekFirst positions at the first key.
func (iter *Merge[Over, Base]) SeekFirst() bool {
	// if iter.fault {
	// 	//deny S10,S11
	// 	return false
	// }
	over := iter.over.SeekFirst()
	base := iter.base.SeekFirst()
	return iter.mergeNext(over, base)
}

// SeekLast positions at the last key.
func (iter *Merge[Over, Base]) SeekLast() bool {
	// if iter.fault {
	// 	//deny S10,S11
	// 	return false
	// }
	over := iter.over.SeekLast()
	base := iter.base.SeekLast()
	return iter.mergePrev(over, base)
}

// Seek positions the iterator at the first key >= the given key.
func (iter *Merge[Over, Base]) Seek(key []byte) bool {
	// if iter.fault {
	// 	//deny S10,S11
	// 	return false
	// }
	over := iter.over.Seek(key)
	base := iter.base.Seek(key)
	return iter.mergeNext(over, base)
}

// mergeNext transitions to the next state after forward operations.
// Transitions from S1,S2,S3,S4,S5,S6,S7,S8,S9,S12 to S1,S2,S5,S6,S7,S10,S11,S12.
func (iter *Merge[Over, Base]) mergeNext(over, base bool) bool {
	if over {
		if base {
			//to S5,S6,S7
			iter.valid = true
			iter.fault = false
			iter.only = false
			if cmp := bytes.Compare(iter.over.Key(), iter.base.Key()); cmp > 0 {
				//to S7
				iter.same = false
				iter.next = true
				iter.cover = false
			} else if cmp < 0 {
				//to S6
				iter.same = false
				iter.next = true
				iter.cover = true
			} else {
				//to S5
				iter.same = true
				// iter.next = true
				iter.cover = true
			}
			return true
		} else if iter.base.Error() != nil {
			//to S11
			iter.valid = false
			iter.fault = true
			// iter.only = true
			// iter.same = false
			// iter.next = true
			iter.cover = false
			return false
		} else {
			//to S1
			iter.valid = true
			iter.fault = false
			iter.only = true
			// iter.same = false
			iter.next = true
			iter.cover = true
			return true
		}
	} else if iter.over.Error() != nil {
		//to S10
		iter.valid = false
		iter.fault = true
		// iter.only = true
		// iter.same = false
		// iter.next = true
		iter.cover = true
		return false
	} else if base {
		//to S2
		iter.valid = true
		iter.fault = false
		iter.only = true
		// iter.same = false
		iter.next = true
		iter.cover = false
		return true
	} else if iter.base.Error() != nil {
		//to S11
		iter.valid = false
		iter.fault = true
		// iter.only = true
		// iter.same = false
		// iter.next = true
		iter.cover = false
		return false
	} else {
		//to S12
		iter.valid = false
		iter.fault = false
		// iter.only = false
		// iter.same = true
		// iter.next = true
		// iter.cover = false
		return false
	}
}

// mergePrev transitions to the next state after backward operations.
// Transitions from S1,S2,S3,S4,S5,S6,S7,S8,S9,S12 to S3,S4,S5,S8,S9,S10,S11,S12.
func (iter *Merge[Over, Base]) mergePrev(over, base bool) bool {
	if over {
		if base {
			//to S5,S8,S9
			iter.valid = true
			iter.fault = false
			iter.only = false
			if cmp := bytes.Compare(iter.over.Key(), iter.base.Key()); cmp < 0 {
				//to S9
				iter.same = false
				iter.next = false
				iter.cover = false
			} else if cmp > 0 {
				//to S8
				iter.same = false
				iter.next = false
				iter.cover = true
			} else {
				//to S5
				iter.same = true
				// iter.next = false
				iter.cover = true
			}
			return true
		} else if iter.base.Error() != nil {
			//to S11
			iter.valid = false
			iter.fault = true
			// iter.only = true
			// iter.same = false
			// iter.next = false
			iter.cover = false
			return false
		} else {
			//to S3
			iter.valid = true
			iter.fault = false
			iter.only = true
			// iter.same = false
			iter.next = false
			iter.cover = true
			return true
		}
	} else if iter.over.Error() != nil {
		//to S10
		iter.valid = false
		iter.fault = true
		// iter.only = true
		// iter.same = false
		// iter.next = false
		iter.cover = true
		return false
	} else if base {
		//to S4
		iter.valid = true
		iter.fault = false
		iter.only = true
		// iter.same = false
		iter.next = false
		iter.cover = false
		return true
	} else if iter.base.Error() != nil {
		//to S11
		iter.valid = false
		iter.fault = true
		// iter.only = true
		// iter.same = false
		// iter.next = false
		iter.cover = false
		return false
	} else {
		//to S12
		iter.valid = false
		iter.fault = false
		// iter.only = false
		// iter.same = true
		// iter.next = false
		// iter.cover = false
		return false
	}
}
