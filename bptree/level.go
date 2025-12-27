package bptree

type Level []struct {
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
