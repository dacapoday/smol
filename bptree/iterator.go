package bptree

import (
	"bytes"

	"github.com/dacapoday/smol/iterator"
	"github.com/dacapoday/smol/overflow"
)

func (bptree *BPTree[B, C]) Iterator() (iter Iterator[B, C]) {
	iter.ator = new(ator[B, C])
	if root := bptree.AcquireRoot(); root != nil {
		iter.ator.Load(bptree.block, root)
	}
	return
}

type Iterator[B ReadOnly, C Checkpoint] struct {
	*ator[B, C]
}

func (iter Iterator[B, C]) Clone() (newIter Iterator[B, C]) {
	newIter.ator = new(ator[B, C])
	if root := iter.ator.Root(); root != nil {
		root.Checkpoint().Acquire()
		newIter.ator.Clone(iter.ator)
	}
	return
}

func (iter Iterator[B, C]) Close() {
	if root := iter.ator.Root(); root != nil {
		root.Checkpoint().Release()
		iter.ator.Close()
	}
}

type ator[B ReadOnly, C Checkpoint] = Reader[B, *Root[C]]

var _ iterator.Iterator = (*Reader[ReadOnly, RootBlock])(nil)

func (reader *Reader[B, R]) Valid() bool {
	return reader.err == null
}

func (reader *Reader[B, R]) Error() error {
	if reader.err == nil {
		return ErrClosed
	}
	if reader.err == null || reader.err == exhausted {
		return nil
	}
	return reader.err
}

func (reader *Reader[B, R]) Key() (key []byte) {
	if reader.err != null {
		return
	}
	key = reader.page.LeafKey(reader.index)
	if len(key) > reader.root.KeyInlineSize() {
		if len(reader.key) != 0 {
			key = reader.key
			return
		}
		var err error
		key, err = overflow.Read(reader.block, reader.key, key)
		if err != nil {
			reader.err = err
			return
		}
		reader.key = key
	}
	return
}

func (reader *Reader[B, R]) Val() (val []byte) {
	if reader.err != null {
		return
	}
	val = reader.page.LeafVal(reader.index)
	if len(val) > reader.root.ValInlineSize() {
		if len(reader.val) != 0 {
			val = reader.val
			return
		}
		var err error
		val, err = overflow.Read(reader.block, reader.val, val)
		if err != nil {
			reader.err = err
			return
		}
		reader.val = val
	}
	return
}

func (reader *Reader[B, R]) Next() bool {
	if reader.err != null {
		return false
	}
	if reader.next() {
		reader.key = reader.key[:0]
		reader.val = reader.val[:0]
		return true
	}
	high := len(reader.level)
	h := high - 1
	for {
		if h < 0 {
			reader.err = exhausted
			return false
		}
		if reader.level.next(h) {
			break
		}
		h--
	}
	var blockID BlockID
	if h == 0 {
		blockID = reader.root.Page().BranchID(reader.level[0].Index)
	} else if err := reader.block.ReadBlock(reader.level[h].BlockID, reader.page, func(block []byte) {
		blockID = Page(block).BranchID(reader.level[h].Index)
	}); err != nil {
		reader.err = err
		return false
	}
	seekFirst := func(block []byte) {
		page := Page(block)
		count := page.Count()
		reader.level[h].BlockID = blockID
		reader.level[h].Count = count
		reader.level[h].Index = 0
		blockID = page.BranchID(0)
	}
	for h++; h < high; h++ {
		if err := reader.block.ReadBlock(blockID, reader.page, seekFirst); err != nil {
			reader.err = err
			return false
		}
	}
	if err := reader.block.ReadBlock(blockID, reader.page, nil); err != nil {
		reader.err = err
		return false
	}
	count := reader.page.Count()
	reader.level[0].BlockID = blockID
	reader.count = count
	reader.index = 0
	reader.key = reader.key[:0]
	reader.val = reader.val[:0]
	return true
}

func (reader *Reader[B, R]) Prev() bool {
	if reader.err != null {
		return false
	}
	if reader.prev() {
		reader.key = reader.key[:0]
		reader.val = reader.val[:0]
		return true
	}
	high := len(reader.level)
	h := high - 1
	for {
		if h < 0 {
			reader.err = exhausted
			return false
		}
		if reader.level.prev(h) {
			break
		}
		h--
	}
	var blockID BlockID
	if h == 0 {
		blockID = reader.root.Page().BranchID(reader.level[0].Index)
	} else if err := reader.block.ReadBlock(reader.level[h].BlockID, reader.page, func(block []byte) {
		blockID = Page(block).BranchID(reader.level[h].Index)
	}); err != nil {
		reader.err = err
		return false
	}
	seekLast := func(block []byte) {
		page := Page(block)
		count := page.Count()
		index := count - 1
		reader.level[h].BlockID = blockID
		reader.level[h].Count = count
		reader.level[h].Index = index
		blockID = page.BranchID(index)
	}
	for h++; h < high; h++ {
		if err := reader.block.ReadBlock(blockID, reader.page, seekLast); err != nil {
			reader.err = err
			return false
		}
	}
	if err := reader.block.ReadBlock(blockID, reader.page, nil); err != nil {
		reader.err = err
		return false
	}
	count := reader.page.Count()
	reader.level[0].BlockID = blockID
	reader.count = count
	reader.index = count - 1
	reader.key = reader.key[:0]
	reader.val = reader.val[:0]
	return true
}

func (reader *Reader[B, R]) SeekFirst() bool {
	// if reader.err != null {
	// 	return false
	// }
	high := len(reader.level)
	if high == 0 {
		if reader.err == nil {
			return false
		}
		count := reader.page.Count()
		if count == 0 {
			reader.err = exhausted
			return false
		}
		reader.count = count
		reader.index = 0
		reader.err = null
		reader.key = reader.key[:0]
		reader.val = reader.val[:0]
		return true
	}
	page := reader.root.Page()
	count := page.Count()
	reader.level[0].Count = count
	reader.level[0].Index = 0
	blockID := page.BranchID(0)
	h := 1
	seekFirst := func(block []byte) {
		page := Page(block)
		count := page.Count()
		reader.level[h].BlockID = blockID
		reader.level[h].Count = count
		reader.level[h].Index = 0
		blockID = page.BranchID(0)
	}
	for ; h < high; h++ {
		reader.err = reader.block.ReadBlock(blockID, reader.page, seekFirst)
		if reader.err != nil {
			return false
		}
	}
	reader.err = reader.block.ReadBlock(blockID, reader.page, nil)
	if reader.err != nil {
		return false
	}
	count = reader.page.Count()
	reader.level[0].BlockID = blockID
	reader.count = count
	reader.index = 0
	reader.err = null
	reader.key = reader.key[:0]
	reader.val = reader.val[:0]
	return true
}

func (reader *Reader[B, R]) SeekLast() bool {
	// if reader.err != null {
	// 	return false
	// }
	high := len(reader.level)
	if high == 0 {
		if reader.err == nil {
			return false
		}
		count := reader.page.Count()
		if count == 0 {
			reader.err = exhausted
			return false
		}
		reader.count = count
		reader.index = count - 1
		reader.err = null
		reader.key = reader.key[:0]
		reader.val = reader.val[:0]
		return true
	}
	page := reader.root.Page()
	count := page.Count()
	index := count - 1
	reader.level[0].Count = count
	reader.level[0].Index = index
	blockID := page.BranchID(index)
	h := 1
	seekLast := func(block []byte) {
		page := Page(block)
		count := page.Count()
		index := count - 1
		reader.level[h].BlockID = blockID
		reader.level[h].Count = count
		reader.level[h].Index = index
		blockID = page.BranchID(index)
	}
	for ; h < high; h++ {
		reader.err = reader.block.ReadBlock(blockID, reader.page, seekLast)
		if reader.err != nil {
			return false
		}
	}
	reader.err = reader.block.ReadBlock(blockID, reader.page, nil)
	if reader.err != nil {
		return false
	}
	count = reader.page.Count()
	index = count - 1
	reader.level[0].BlockID = blockID
	reader.count = count
	reader.index = index
	reader.err = null
	reader.key = reader.key[:0]
	reader.val = reader.val[:0]
	return true
}

func (reader *Reader[B, R]) Seek(key []byte) bool {
	// if reader.err != null {
	// 	return false
	// }
	cursor := cursor[B, R]{
		Reader:        reader,
		key:           key,
		keyInlineSize: reader.root.KeyInlineSize(),
	}
	high := len(reader.level)
	if high == 0 {
		if reader.err == nil {
			return false
		}
		page := reader.page
		count := page.Count()
		if count == 0 {
			reader.err = exhausted
			return false
		}
		index := cursor.searchLeaf(count, page)
		if cursor.err != nil {
			reader.err = cursor.err
			return false
		}
		if index == count {
			reader.err = exhausted
			return false
		}
		reader.count = count
		reader.index = index
		reader.err = null
		reader.key = reader.key[:0]
		reader.val = reader.val[:0]
		return true
	}
	page := reader.root.Page()
	count := page.Count()
	index := cursor.searchBranch(count, page)
	if cursor.err != nil {
		reader.err = cursor.err
		return false
	}
	if index == count {
		reader.err = exhausted
		return false
	}
	reader.level[0].Count = count
	reader.level[0].Index = index
	blockID := page.BranchID(index)
	h := 1
	seek := func(block []byte) {
		page := Page(block)
		count := page.Count()
		index := cursor.searchBranch(count-1, page)
		reader.level[h].BlockID = blockID
		reader.level[h].Count = count
		reader.level[h].Index = index
		blockID = page.BranchID(index)
	}
	for ; h < high; h++ {
		reader.err = reader.block.ReadBlock(blockID, reader.page, seek)
		if reader.err != nil {
			return false
		}
		if cursor.err != nil {
			reader.err = cursor.err
			return false
		}
	}
	reader.err = reader.block.ReadBlock(blockID, reader.page, nil)
	if reader.err != nil {
		return false
	}
	page = reader.page
	count = page.Count()
	index = cursor.searchLeaf(count-1, page)
	if cursor.err != nil {
		reader.err = cursor.err
		return false
	}
	reader.level[0].BlockID = blockID
	reader.count = count
	reader.index = index
	reader.err = null
	reader.key = reader.key[:0]
	reader.val = reader.val[:0]
	return true
}

// cursor should stack-only; no escape
type cursor[B ReadOnly, R RootBlock] struct {
	*Reader[B, R]
	key           []byte
	page          Page
	err           error
	keyInlineSize int
}

func (cursor *cursor[B, R]) searchLeaf(count uint16, page Page) (index uint16) {
	cursor.page = page
	index = search(count, cursor.leaf)
	// cursor.page = nil
	return
}

func (cursor *cursor[B, R]) leaf(i uint16) int {
	leafKey := cursor.page.LeafKey(i)
	if len(leafKey) > cursor.keyInlineSize {
		// TODO: compare with overflow head first
		if cursor.err != nil {
			return 0
		}
		leafKey, cursor.err = overflow.Read(cursor.block, cursor.Reader.key, leafKey)
		if cursor.err != nil {
			return 0
		}
		cursor.Reader.key = leafKey
	}
	return bytes.Compare(cursor.key, leafKey)
}

func (cursor *cursor[B, R]) searchBranch(count uint16, page Page) (index uint16) {
	cursor.page = page
	index = search(count, cursor.branch)
	// cursor.page = nil
	return
}

func (cursor *cursor[B, R]) branch(i uint16) int {
	branchKey := cursor.page.BranchKey(i)
	if len(branchKey) > cursor.keyInlineSize {
		// TODO: compare with overflow head first
		if cursor.err != nil {
			return 0
		}
		branchKey, cursor.err = overflow.Read(cursor.block, cursor.Reader.key, branchKey)
		if cursor.err != nil {
			return 0
		}
		cursor.Reader.key = branchKey
	}
	return bytes.Compare(cursor.key, branchKey)
}
