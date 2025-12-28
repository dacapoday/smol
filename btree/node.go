package btree

import (
	"sort"
	"strings"
)

const order = 6 // min: 2
const half = (order + 1) / 2
const double = 2*order + 1

type node struct {
	count int
	keys  [order]string
	vals  [order]string
	nodes [order]*node
	last  *node
}

func (node *node) key(i int) string {
	return node.keys[i]
}

func (node *node) val(i int) string {
	return node.vals[i]
}

func (node *node) node(i int) *node {
	if i == node.count {
		return node.last
	}
	return node.nodes[i]
}

func (node *node) find(key string) (int, bool) {
	return sort.Find(node.count, func(i int) int {
		return strings.Compare(key, node.keys[i])
	})
}

func (node *node) update(i int, val string) {
	node.vals[i] = val
}

func (node *node) insert(i int, entry *entry) {
	if i != node.count {
		l := i + 1
		copy(node.keys[l:], node.keys[i:node.count])
		copy(node.vals[l:], node.vals[i:node.count])
		copy(node.nodes[l:], node.nodes[i:node.count])
	}
	node.count++
	node.keys[i] = entry.key
	node.vals[i] = entry.val
	node.nodes[i] = entry.node
}

func (node *node) items(yield func(key, val []byte) bool) bool {
	if node.last == nil {
		for i := 0; i < node.count; i++ {
			if !yield(s2b(node.keys[i]), s2b(node.vals[i])) {
				return false
			}
		}
		return true
	}
	for i := 0; i < node.count; i++ {
		if !node.nodes[i].items(yield) {
			return false
		}
		if !yield(s2b(node.keys[i]), s2b(node.vals[i])) {
			return false
		}
	}
	return node.last.items(yield)
}
