package bptree

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

// Page represents a B+ tree page.
// Use IsLeaf to distinguish branch and leaf pages, then call the respective
// methods (Branch or Leaf prefix). Incorrect calls are undefined behavior.
type Page []byte

// Page use LittleEndian encoding
// EmptyRootLeafPage is nil or empty []byte
// LeafPage is (Count>0 and IsLeaf){byte[0:2]:Head, byte[2:4]:Size, byte[4:4+Count*2]:offset, byte[4+Count*2:n]:LeafItem}
// LeafItem is {uvarint,Key,Val}, uvarint is key's length size
// BranchPage is (Count>0 and not IsLeaf){byte[0:2]:Head, byte[2:4]:Size, byte[4:4+Count*2]:offset, byte[4+Count*2:n]:BranchItem}
// BranchItem is {BlockID,Key}
// Head is MSB{bit0:reserved, bit1:IsLeaf, bit[2:]:Count}LSB

const HeadSize = 4 // Head + Size

// Size returns the total size of the page in bytes.
func (page Page) Size() int {
	if len(page) < HeadSize {
		return len(page)
	}
	return int(binary.LittleEndian.Uint16(page[2:])) + HeadSize
}

// Count returns the number of items in the page.
func (page Page) Count() uint16 { // branch max: 8223, leaf max: 13158
	if len(page) < HeadSize {
		return 0
	}
	return binary.LittleEndian.Uint16(page) & 0x3FFF
}

// IsLeaf reports whether the page is a leaf page.
func (page Page) IsLeaf() bool {
	if len(page) < HeadSize {
		return true
	}
	return page[1]&0x40 == 0
}

func (page Page) item(index uint16) []byte {
	offset := 2*index + HeadSize
	beg := binary.LittleEndian.Uint16(page[offset:]) + HeadSize
	end := binary.LittleEndian.Uint16(page[offset-2:]) + HeadSize
	return page[beg:end]
}

// LeafKey returns the key at the given index in a leaf page.
// Only call this method on leaf pages (when IsLeaf returns true).
func (page Page) LeafKey(index uint16) []byte {
	item := page.item(index)
	klen, klen_size := binary.Uvarint(item)
	if klen_size <= 0 {
		return nil
	}
	return item[klen_size : uint64(klen_size)+klen]
}

// LeafVal returns the value at the given index in a leaf page.
// Only call this method on leaf pages (when IsLeaf returns true).
func (page Page) LeafVal(index uint16) []byte {
	item := page.item(index)
	klen, klen_size := binary.Uvarint(item)
	if klen_size <= 0 {
		return nil
	}
	return item[uint64(klen_size)+klen:]
}

// LeafItems returns an iterator for key-value pairs in the range [beg, end) of a leaf page.
// Only call this method on leaf pages (when IsLeaf returns true).
func (page Page) LeafItems(beg, end uint16) LeafItems {
	return func(yield func([]byte, []byte) bool) {
		for i := beg; i < end; i++ {
			if !yield(page.LeafKey(i), page.LeafVal(i)) {
				return
			}
		}
	}
}

func leafItemSize(klen, vlen int) int {
	// offset + klen_size + klen + vlen
	return 2 + sizeUvarint(klen) + klen + vlen
}

func encodeLeafPage(buffer []byte, items LeafItems) (key []byte) {
	// if len(buffer) > 65536 {
	// 	panic(errors.New("buffer too large"))
	// }
	// if len(buffer) < HeadSize {
	// 	panic(errors.New("buffer too small"))
	// }

	beg := len(buffer) - HeadSize
	end := beg
	binary.LittleEndian.PutUint16(buffer[2:], uint16(end))

	body := buffer[HeadSize:]
	var offset, klen int
	var item, val []byte
	for key, val = range items {
		offset += 2
		klen = len(key)
		beg -= sizeUvarint(klen) + klen + len(val)

		if offset > beg {
			panic(errors.New("buffer too small"))
		}

		binary.LittleEndian.PutUint16(body[offset-2:], uint16(beg))

		item = body[beg:end]
		item = item[binary.PutUvarint(item, uint64(klen)):]
		copy(item, key)
		copy(item[klen:], val)
		end = beg
	}

	if offset == 0 {
		panic(errors.New("empty leaf page"))
	}
	binary.LittleEndian.PutUint16(buffer, uint16(offset/2)) // count
	return
}

// BranchKey returns the key at the given index in a branch page.
// Only call this method on branch pages (when IsLeaf returns false).
func (page Page) BranchKey(index uint16) []byte {
	return page.item(index)[4:]
}

// BranchID returns the block ID at the given index in a branch page.
// Only call this method on branch pages (when IsLeaf returns false).
func (page Page) BranchID(index uint16) BlockID {
	return binary.LittleEndian.Uint32(page.item(index))
}

// BranchItems returns an iterator for key-blockID pairs in the range [beg, end) of a branch page.
// Only call this method on branch pages (when IsLeaf returns false).
func (page Page) BranchItems(beg, end uint16) BranchItems {
	return func(yield func([]byte, BlockID) bool) {
		for i := beg; i < end; i++ {
			if !yield(page.BranchKey(i), page.BranchID(i)) {
				return
			}
		}
	}
}

func branchItemSize(klen int) int {
	// offset + BlockID + klen
	return 2 + 4 + klen
}

func encodeBranchPage(buffer []byte, items BranchItems) (key []byte) {
	// if len(buffer) > 65536 {
	// 	panic(errors.New("buffer too large"))
	// }
	// if len(buffer) < HeadSize {
	// 	panic(errors.New("buffer too small"))
	// }

	beg := len(buffer) - HeadSize
	end := beg
	binary.LittleEndian.PutUint16(buffer[2:], uint16(end))

	body := buffer[HeadSize:]
	var offset int
	var item []byte
	var blockID BlockID
	for key, blockID = range items {
		offset += 2
		beg -= 4 + len(key)

		if offset > beg {
			panic(errors.New("buffer too small"))
		}

		binary.LittleEndian.PutUint16(body[offset-2:], uint16(beg))

		item = body[beg:end]
		binary.LittleEndian.PutUint32(item, blockID)
		copy(item[4:], key)
		end = beg
	}

	if offset == 0 {
		panic(errors.New("empty branch page"))
	}
	binary.LittleEndian.PutUint16(buffer, uint16(offset/2)|0x4000) // count
	return
}

type items[V BlockID | []byte] interface {
	~func(func([]byte, V) bool)
	ItemSize(func(int) bool)
	encode([]byte) []byte
}

// LeafItems is an iter for leaf page key-value pairs.
type LeafItems func(func([]byte, []byte) bool)

func (items LeafItems) ItemSize(yield func(int) bool) {
	for k, v := range items {
		if !yield(leafItemSize(len(k), len(v))) {
			return
		}
	}
}

func (items LeafItems) encode(buffer []byte) (key []byte) {
	return encodeLeafPage(buffer, items)
}

// BranchItems is an iter for branch page key-blockID pairs.
type BranchItems func(func([]byte, BlockID) bool)

func (items BranchItems) ItemSize(yield func(int) bool) {
	for k := range items {
		if !yield(branchItemSize(len(k))) {
			return
		}
	}
}

func (items BranchItems) encode(buffer []byte) (key []byte) {
	return encodeBranchPage(buffer, items)
}

func search(n uint16, f func(uint16) int) uint16 {
	var i, j uint16 = 0, n
	for i < j {
		h := (i + j) >> 1
		if f(h) > 0 {
			i = h + 1
		} else {
			j = h
		}
	}
	return i
}

func find(n uint16, f func(uint16) int) (uint16, bool) {
	var i, j uint16 = 0, n
	for i < j {
		h := (i + j) >> 1
		if f(h) > 0 {
			i = h + 1
		} else {
			j = h
		}
	}
	return i, i < n && f(i) == 0
}

func s2b(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func b2s(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// InlineSize calculates the maximum inline sizes for keys and values in a page.
// Parameters:
//   - pageSize: the size of a page in bytes
//   - branchFactor: the minimum branching factor for branch pages
//   - maxKeyOverflowSize: the maximum size of a key in overflow storage
//   - maxValOverflowSize: the maximum size of a value in overflow storage
//
// Returns the maximum inline sizes for keys and values that can fit in a page.
func InlineSize(pageSize, branchFactor, maxKeyOverflowSize, maxValOverflowSize int) (keyInlineSize, valInlineSize int) {
	keyOverflowHeadSize := (pageSize-HeadSize)/branchFactor - 2 - 4           // -2 for offset, -4 for BranchID
	keyInlineSize = keyOverflowHeadSize - sizeUvarint(maxKeyOverflowSize) - 4 // -4 for OverflowID

	valInlineSize = pageSize - HeadSize - 2 - sizeUvarint(keyOverflowHeadSize) - keyOverflowHeadSize - sizeUvarint(maxValOverflowSize) - 4 // -2 for offset, -4 for OverflowID
	return
}

func sizeUvarint(x int) (size int) {
	switch {
	case x < 128: // 1<<7
		return 1
	case x < 16384: // 1<<14
		return 2
	case x < 2097152: // 1<<21
		return 3
	case x < 268435456: // 1<<28
		return 4
	case x < 34359738368: // 1<<35
		return 5
	case x < 4398046511104: // 1<<42
		return 6
	case x < 562949953421312: // 1<<49
		return 7
	case x < 72057594037927936: // 1<<56
		return 8
	default:
		return 9
	}
}

// func sizeUvarint(x uint64) (size uint64) {
// 	for {
// 		size++
// 		x >>= 7
// 		if x == 0 {
// 			return
// 		}
// 	}
// }
