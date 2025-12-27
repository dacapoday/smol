package bptree

import (
	"fmt"
	"math"
	"sync"
)

type BPTree[B Block[C], C Checkpoint] struct {
	block B
	root  *Root[C]
	view  sync.RWMutex
	mutex sync.Mutex
}

func (bptree *BPTree[B, C]) Block() B {
	return bptree.block
}

func (bptree *BPTree[B, C]) Load(block B, entry []byte, ckpt C) (err error) {
	bptree.mutex.Lock()
	defer bptree.mutex.Unlock()
	bptree.view.Lock()
	defer bptree.view.Unlock()

	root, err := loadRoot(block, entry, ckpt)
	if err != nil {
		return
	}

	bptree.root = root
	bptree.block = block
	return
}

func (bptree *BPTree[B, C]) Close() (err error) {
	bptree.mutex.Lock()
	defer bptree.mutex.Unlock()
	bptree.view.Lock()
	defer bptree.view.Unlock()

	if bptree.root == nil {
		return
	}

	bptree.root.ckpt.Release()
	bptree.root = nil

	err = bptree.block.Close()
	var nilBlock B
	bptree.block = nilBlock
	return
}

func (bptree *BPTree[B, C]) Get(key []byte) (val []byte, err error) {
	root := bptree.AcquireRoot()
	if root == nil {
		err = ErrClosed
		return
	}
	defer root.Checkpoint().Release()

	return Get(bptree.block, root, key, nil)
}

func (bptree *BPTree[B, C]) Set(key []byte, val []byte) (err error) {
	return bptree.WriteSortedChanges(func(yield func([]byte, []byte) bool) { yield(key, val) }, math.MaxUint32)
}

func (bptree *BPTree[B, C]) WriteSortedChanges(sortedChanges func(func([]byte, []byte) bool), maxPages uint32) (err error) {
	bptree.mutex.Lock()
	defer bptree.mutex.Unlock()

	oldRoot := bptree.root
	if oldRoot == nil {
		return ErrClosed
	}

	high, page, err := WriteSortedChanges(bptree.block, oldRoot, sortedChanges, maxPages)
	if err != nil {
		bptree.block.Rollback()
		return
	}

	ckpt, err := bptree.block.Commit(page)
	if err != nil {
		return
	}

	newRoot := &Root[C]{
		ckpt: ckpt,
		high: high,
		page: page,
		klen: oldRoot.klen,
		vlen: oldRoot.vlen,
	}

	bptree.view.Lock()
	bptree.root = newRoot
	oldRoot.ckpt.Release()
	bptree.view.Unlock()
	return
}

func (bptree *BPTree[B, C]) AcquireRoot() (root *Root[C]) {
	bptree.view.RLock()
	if root = bptree.root; root != nil {
		root.ckpt.Acquire()
	}
	bptree.view.RUnlock()
	return
}

type Root[C Checkpoint] struct {
	ckpt C
	page Page
	klen uint16
	vlen uint16
	high uint8
}

func loadRoot[B Block[C], C Checkpoint](block B, entry []byte, ckpt C) (*Root[C], error) {
	root := new(Root[C])
	if entrySize := len(entry); entrySize != 0 {
		page := Page(entry)
		if page.Count() == 0 {
			return nil, fmt.Errorf("entry is %w", ErrUnsupported)
		}

		high, err := High(block, page)
		if err != nil {
			return nil, fmt.Errorf("High: %w", err)
		}

		root.high = high
		root.page = page
	}
	{
		pageSize := block.PageSize()
		maxOverflowSize := math.MaxUint32 * pageSize // maxOverflowSize := (math.MaxUint32 - 2) * (pageSize - HeadSize - 4)
		klen, vlen := InlineSize(pageSize, 5, maxOverflowSize, maxOverflowSize)

		root.klen = uint16(klen)
		root.vlen = uint16(vlen)
	}
	root.ckpt = ckpt

	return root, nil
}

func (root *Root[C]) Checkpoint() C {
	return root.ckpt
}

var _ RootBlock = (*Root[Checkpoint])(nil)

func (root *Root[C]) High() uint8 {
	return root.high
}

func (root *Root[C]) Page() Page {
	return root.page
}

func (root *Root[C]) KeyInlineSize() int {
	return int(root.klen)
}

func (root *Root[C]) ValInlineSize() int {
	return int(root.vlen)
}
