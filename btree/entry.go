package btree

type entry struct {
	key, val string
	node     *node
}

func (e *entry) set(n *node) bool {
	var cursors []cursor
	var index int
	var found bool
	var next *node
	for {
		index, found = n.find(e.key)
		if found {
			n.update(index, e.val)
			return true
		}
		next = n.node(index)
		if next == nil {
			break
		}
		cursors = append(cursors, cursor{n, index})
		n = next
	}
	if e.insert(index, n) {
		return true
	}
	for i := len(cursors) - 1; i >= 0; i-- {
		if e.insert(cursors[i].index, cursors[i].node) {
			return true
		}
	}
	return false
}

func (e *entry) insert(i int, n *node) bool {
	if n.count < order {
		n.insert(i, e)
		return true
	}
	e.split(i, n)
	return false
}

func (e *entry) split(i int, n *node) {
	const total = order + 1
	var keys [total]string
	var vals [total]string
	var nodes [total]*node
	{
		copy(keys[:i], n.keys[:i])
		copy(vals[:i], n.vals[:i])
		copy(nodes[:i], n.nodes[:i])

		keys[i] = e.key
		vals[i] = e.val
		nodes[i] = e.node

		l := i + 1
		copy(keys[l:], n.keys[i:])
		copy(vals[l:], n.vals[i:])
		copy(nodes[l:], n.nodes[i:])
	}

	newn := new(node)
	copy(newn.keys[:], keys[:half])
	copy(newn.vals[:], vals[:half])
	copy(newn.nodes[:], nodes[:half])
	newn.count = half
	newn.last = nodes[half]

	e.key = keys[half]
	e.val = vals[half]
	e.node = newn

	const r = half + 1
	copy(n.keys[:], keys[r:])
	copy(n.vals[:], vals[r:])
	copy(n.nodes[:], nodes[r:])
	n.count = order - half
}
