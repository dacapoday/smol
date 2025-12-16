// Package btree provides an append-only, in-memory B-tree implementation with iterator support.
package btree

import (
	"sort"
	"strings"
	"unsafe"
)

// BTree is an append-only, in-memory B-tree for storing key-value pairs in lexicographic order.
// Not thread-safe.
//
// Append-only means only updating a key's value is allowed (even to nil).
// This enables two common use cases:
//   - Change tracking: Set(key, nil) marks deletion
//   - Use as a Set: Ignore values, use Get's found to check key existence
//
// Important: BTree stores references to key/val byte slices, not copies.
// Do not modify the underlying arrays after calling Set.
//
// Example usage:
//
//	var btree BTree
//	btree.Set([]byte("key"), []byte("value"))
//	val, found := btree.Get([]byte("key"))  // val == "value", found == true
//
//	for key, val := range btree.Items {
//		fmt.Printf("Item: %s = %s\n", key, val)
//	}
//
//	btree.Reset()  // Clear all data
type BTree struct {
	items   []item
	nodes   []*node
	last    *node
	version uint64
}

type item = struct {
	key, val string
}

// Reset clears all data.
func (btree *BTree) Reset() {
	btree.items = nil
	btree.nodes = nil
	btree.last = nil
	btree.version++
}

// Set updates the value for a key (inserts if key doesn't exist).
// Passing nil as val updates the value to nil, which can represent deletion.
func (btree *BTree) Set(key, val []byte) {
	btree.version++
	btree.set(b2s(key), b2s(val))
}

// Get retrieves the value for a key.
// When val is nil and found is true, the key exists but its value was set to nil.
// When found is false, the key doesn't exist in the BTree.
func (btree *BTree) Get(key []byte) (val []byte, found bool) {
	v, found := btree.get(b2s(key))
	return s2b(v), found
}

// Empty returns true if BTree has no keys.
func (btree *BTree) Empty() bool {
	return len(btree.items) == 0
}

// Items implements iter.Seq2[[]byte, []byte], iterating all key-value pairs in lexicographic order.
// Includes deleted keys (val==nil). Returned slices are valid only within the yield call.
func (btree *BTree) Items(yield func(key, val []byte) bool) {
	if btree.last == nil {
		for i := 0; i < len(btree.items); i++ {
			if !yield(s2b(btree.items[i].key), s2b(btree.items[i].val)) {
				return
			}
		}
		return
	}
	for i := 0; i < len(btree.items); i++ {
		if !btree.nodes[i].items(yield) {
			return
		}
		if !yield(s2b(btree.items[i].key), s2b(btree.items[i].val)) {
			return
		}
	}
	btree.last.items(yield)
}

func s2b(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func b2s(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func (btree *BTree) set(key, val string) {
	entry := entry{key, val, nil}
	index, found := btree.find(entry.key)
	if found {
		btree.update(index, entry.val)
		return
	}
	next := btree.node(index)
	if next == nil {
		btree.insertItem(index, &entry)
		return
	}
	if entry.set(next) {
		return
	}
	btree.insertEntry(index, &entry)
}

func (btree *BTree) get(key string) (val string, found bool) {
	index, found := btree.find(key)
	if found {
		return btree.val(index), true
	}

	node := btree.node(index)
	for node != nil {
		index, found = node.find(key)
		if found {
			return node.val(index), true
		}
		node = node.node(index)
	}
	return
}

func (btree *BTree) key(i int) string {
	return btree.items[i].key
}

func (btree *BTree) val(i int) string {
	return btree.items[i].val
}

func (btree *BTree) node(i int) *node {
	if i >= len(btree.nodes) {
		return btree.last
	}
	return btree.nodes[i]
}

func (btree *BTree) find(key string) (int, bool) {
	return sort.Find(len(btree.items), func(i int) int {
		return strings.Compare(key, btree.items[i].key)
	})
}

func (btree *BTree) update(i int, val string) {
	btree.items[i].val = val
}

func (btree *BTree) insertItem(i int, entry *entry) {
	count := len(btree.items)

	if i == count {
		btree.items = append(btree.items, item{entry.key, entry.val})
	} else {
		btree.items = append(btree.items, item{})

		l := i + 1
		copy(btree.items[l:], btree.items[i:count])

		btree.items[i] = item{entry.key, entry.val}
	}

	if len(btree.items) == double {
		lnode := new(node)
		for i := range order {
			lnode.keys[i] = btree.items[i].key
			lnode.vals[i] = btree.items[i].val
		}
		lnode.count = order

		rnode := new(node)
		for i := range order {
			r := i + order + 1
			rnode.keys[i] = btree.items[r].key
			rnode.vals[i] = btree.items[r].val
		}
		rnode.count = order

		btree.items[0] = btree.items[order]
		btree.items = btree.items[:1]
		btree.nodes = []*node{lnode}
		btree.last = rnode
	}
}

func (btree *BTree) insertEntry(i int, entry *entry) {
	count := len(btree.items)

	if i == count {
		btree.items = append(btree.items, item{entry.key, entry.val})
		btree.nodes = append(btree.nodes, entry.node)
	} else {
		btree.items = append(btree.items, item{})
		btree.nodes = append(btree.nodes, nil)

		l := i + 1
		copy(btree.items[l:], btree.items[i:count])
		copy(btree.nodes[l:], btree.nodes[i:count])

		btree.items[i] = item{entry.key, entry.val}
		btree.nodes[i] = entry.node
	}

	if len(btree.items) == double {
		lnode := new(node)
		for i := range order {
			lnode.keys[i] = btree.items[i].key
			lnode.vals[i] = btree.items[i].val
		}
		copy(lnode.nodes[:], btree.nodes[:order])
		lnode.count = order
		lnode.last = btree.nodes[order]

		rnode := new(node)
		for i := range order {
			r := i + order + 1
			rnode.keys[i] = btree.items[r].key
			rnode.vals[i] = btree.items[r].val
		}
		copy(rnode.nodes[:], btree.nodes[order+1:])
		rnode.count = order
		rnode.last = btree.last

		btree.items[0] = btree.items[order]
		btree.items = btree.items[:1]
		btree.nodes[0] = lnode
		btree.nodes = btree.nodes[:1]
		btree.last = rnode
	}
}
