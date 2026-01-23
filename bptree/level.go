// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package bptree

// Level represents a path from root to leaf in the B+ tree, defining the cursor
// position within the tree.
//
// The slice contains branch pages only: level[0] is root, level[1] is the first
// branch below root, down to level[len-1] at the deepest branch.
//
// Each element contains:
//   - BlockID: block ID of the page at this level
//   - Count:   total entries in this page
//   - Index:   child index pointing to the next level
//
// Note: level[0].BlockID stores the leaf page's block ID, since the root page has
// no block ID (it's embedded in the tree metadata).
type Level []level

type level = struct {
	BlockID BlockID
	Count   uint16
	Index   uint16
}

func (l Level) first() bool {
	for i := range l {
		if l[i].Index != 0 {
			return false
		}
	}
	return true
}

func (l Level) last() bool {
	for i := range l {
		if l[i].Index != l[i].Count-1 {
			return false
		}
	}
	return true
}

func (l Level) next(i int) bool {
	l[i].Index++
	if l[i].Index < l[i].Count {
		return true
	}
	l[i].Index--
	return false
}

func (l Level) prev(i int) bool {
	if l[i].Index == 0 {
		return false
	}
	l[i].Index--
	return true
}

func (l Level) nextTo(r Level) (nextTo bool) {
	nextTo, _ = l.compare(r)
	return
}

func (l Level) compare(r Level) (nextTo bool, samePage bool) {
	// if len(l) != len(r) {
	// 	return false
	// }
	i := len(l) - 1
	if i == 0 {
		if l[0].Index+1 == r[0].Index {
			return true, true
		}
		return false, true
	}
	if l[i].BlockID == r[i].BlockID {
		if l[i].Index+1 == r[i].Index {
			return true, true
		}
		return false, true
	}
	if r[i].Index != 0 {
		return false, false
	}
	if l[i].Index+1 != l[i].Count {
		return false, false
	}
	for i--; i > 0; i-- {
		if l[i].BlockID == r[i].BlockID {
			if l[i].Index+1 == r[i].Index {
				return true, false
			}
			return false, false
		}
		if r[i].Index != 0 {
			return false, false
		}
		if l[i].Index+1 != l[i].Count {
			return false, false
		}
	}
	if l[0].Index+1 == r[0].Index {
		return true, false
	}
	return false, false
}
