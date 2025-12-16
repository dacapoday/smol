package btree

// Iter creates an iterator that stays synchronized with the BTree (not a snapshot).
// Call SeekFirst, SeekLast, or Seek to position it before use.
func (btree *BTree) Iter() Iter {
	return &iter{
		root:    btree,
		cursors: nil,
		key:     "",
		version: btree.version,
		index:   len(btree.items),
	}
}

// Iter is an iterator over BTree. Do not compare with nil or rely on pointer semantics.
// The implementation may change (e.g., to a struct wrapping a pointer).
type Iter = *iter

type iter struct {
	root    *BTree
	cursors []cursor
	key     string
	version uint64
	index   int
}

type cursor = struct {
	node  *node
	index int
}

// Clone creates an independent copy of the iterator at its current position.
func (it Iter) Clone() Iter {
	return &iter{
		root:    it.root,
		cursors: append([]cursor(nil), it.cursors...),
		key:     it.key,
		version: it.version,
		index:   it.index,
	}
}

func (it Iter) sync() bool {
	if len(it.root.items) == 0 {
		it.version = it.root.version
		it.cursors = it.cursors[:0]
		it.index = 0
		it.key = ""
		return false
	}

	return it.seek(it.key)
}

// Valid returns true if positioned at a valid key-value pair.
func (it Iter) Valid() bool {
	if it.version != it.root.version {
		return it.sync()
	}

	if len(it.cursors) == 0 {
		return it.index < len(it.root.items)
	}

	return true
}

// Error exists for Iterator interface compatibility.
func (it Iter) Error() error {
	return nil
}

// Key returns the current key, or nil if invalid.
// Returned slice is valid only until the next method call.
func (it Iter) Key() []byte {
	return s2b(it.key)
}

// Val returns the current value, or nil if invalid or deleted.
// Returned slice is valid only until the next method call.
func (it Iter) Val() []byte {
	if it.version != it.root.version {
		if !it.sync() {
			return nil
		}
	}

	if len(it.cursors) == 0 {
		if it.index >= len(it.root.items) {
			return nil
		}
		return s2b(it.root.val(it.index))
	}

	cursor := &it.cursors[len(it.cursors)-1]
	return s2b(cursor.node.val(cursor.index))
}

// Next advances to the next key. Returns false if no more items.
func (it Iter) Next() bool {
	if it.version != it.root.version {
		if !it.sync() {
			return false
		}
	}

	var node, next *node
	if len(it.cursors) == 0 {
		if it.index >= len(it.root.items) {
			return false
		}

		it.index++
		node = it.root.node(it.index)
		if node == nil {
			if it.index < len(it.root.items) {
				it.key = it.root.key(it.index)
				return true
			}
			it.key = ""
			return false
		}
	} else {
		l := len(it.cursors) - 1
		c := &it.cursors[l]
		c.index++
		node = c.node.node(c.index)
		if node == nil {
			if c.index < c.node.count {
				it.key = c.node.key(c.index)
				return true
			}
			for l--; l >= 0; l-- {
				c = &it.cursors[l]
				if c.index < c.node.count {
					it.cursors = it.cursors[:l+1]
					it.key = c.node.key(c.index)
					return true
				}
			}
			it.cursors = it.cursors[:0]
			if it.index < len(it.root.items) {
				it.key = it.root.key(it.index)
				return true
			}
			it.key = ""
			return false
		}
	}
	for {
		it.cursors = append(it.cursors, cursor{node, 0})
		next = node.node(0)
		if next == nil {
			it.key = node.key(0)
			return true
		}
		node = next
	}
}

// Prev moves to the previous key. Returns false if no more items.
func (it Iter) Prev() bool {
	if it.version != it.root.version {
		if !it.sync() {
			return false
		}
	}

	var node *node
	if len(it.cursors) == 0 {
		if it.index >= len(it.root.items) {
			return false
		}

		node = it.root.node(it.index)
		if node == nil {
			if it.index > 0 {
				it.index--
				it.key = it.root.key(it.index)
				return true
			}
			it.index = len(it.root.items)
			it.key = ""
			return false
		}
	} else {
		l := len(it.cursors) - 1
		c := &it.cursors[l]
		node = c.node.node(c.index)
		if node == nil {
			if c.index > 0 {
				c.index--
				it.key = c.node.key(c.index)
				return true
			}
			for l--; l >= 0; l-- {
				c = &it.cursors[l]
				if c.index > 0 {
					c.index--
					it.cursors = it.cursors[:l+1]
					it.key = c.node.key(c.index)
					return true
				}
			}
			it.cursors = it.cursors[:0]
			if it.index > 0 {
				it.index--
				it.key = it.root.key(it.index)
				return true
			}
			it.index = len(it.root.items)
			it.key = ""
			return false
		}
	}
	for node.last != nil {
		it.cursors = append(it.cursors, cursor{node, node.count})
		node = node.last
	}
	index := node.count - 1
	it.cursors = append(it.cursors, cursor{node, index})
	it.key = node.key(index)
	return true
}

// SeekFirst positions the iterator at the first key. Returns false if BTree is empty.
func (it Iter) SeekFirst() bool {
	if len(it.root.items) == 0 {
		it.version = it.root.version
		it.cursors = it.cursors[:0]
		it.index = 0
		it.key = ""
		return false
	}

	it.version = it.root.version
	it.cursors = it.cursors[:0]

	it.index = 0
	node := it.root.node(0)
	if node == nil {
		it.key = it.root.key(0)
		return true
	}

	for {
		it.cursors = append(it.cursors, cursor{node, 0})
		next := node.node(0)
		if next == nil {
			it.key = node.key(0)
			return true
		}
		node = next
	}
}

// SeekLast positions the iterator at the last key. Returns false if BTree is empty.
func (it Iter) SeekLast() bool {
	if len(it.root.items) == 0 {
		it.version = it.root.version
		it.cursors = it.cursors[:0]
		it.index = 0
		it.key = ""
		return false
	}

	it.version = it.root.version
	it.cursors = it.cursors[:0]

	node := it.root.last
	if node == nil {
		it.index = len(it.root.items) - 1
		it.key = it.root.key(it.index)
		return true
	}
	it.index = len(it.root.items)

	for node.last != nil {
		it.cursors = append(it.cursors, cursor{node, node.count})
		node = node.last
	}

	index := node.count - 1
	it.cursors = append(it.cursors, cursor{node, index})
	it.key = node.key(index)
	return true
}

// Seek positions the iterator at the first key >= the given key.
// Returns false if no such key exists.
func (it Iter) Seek(key []byte) bool {
	if len(it.root.items) == 0 {
		it.version = it.root.version
		it.cursors = it.cursors[:0]
		it.index = 0
		it.key = ""
		return false
	}

	return it.seek(b2s(key))
}

func (it Iter) seek(key string) bool {
	it.version = it.root.version
	it.cursors = it.cursors[:0]

	index, found := it.root.find(key)
	it.index = index
	if found {
		it.key = it.root.key(index)
		return true
	}
	node := it.root.node(index)
	if node == nil {
		if index < len(it.root.items) {
			it.key = it.root.key(index)
			return true
		}
		it.key = ""
		return false
	}

	for {
		index, found = node.find(key)
		it.cursors = append(it.cursors, cursor{node, index})
		if found {
			it.key = node.key(index)
			return true
		}
		next := node.node(index)
		if next != nil {
			node = next
			continue
		}
		if index < node.count {
			it.key = node.key(index)
			return true
		}
		for l := len(it.cursors) - 1; l >= 0; l-- {
			c := &it.cursors[l]
			if c.index < c.node.count {
				it.cursors = it.cursors[:l+1]
				it.key = c.node.key(c.index)
				return true
			}
		}
		it.cursors = it.cursors[:0]
		if it.index < len(it.root.items) {
			it.key = it.root.key(it.index)
			return true
		}
		it.key = ""
		return false
	}
}
