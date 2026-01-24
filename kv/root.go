package kv

import (
	"fmt"
	"math"

	"github.com/dacapoday/smol/block"
	"github.com/dacapoday/smol/bptree"
)

// Root represents a snapshot of the B+ tree root metadata.
type Root struct {
	page bptree.Page
	klen uint16
	vlen uint16
	high uint8
}

func loadRoot[F File](blk *block.Heap[F], entry []byte) (*Root, error) {
	root := new(Root)
	if entrySize := len(entry); entrySize != 0 {
		page := bptree.Page(entry)
		if page.Count() == 0 {
			return nil, fmt.Errorf("entry is %w", bptree.ErrUnsupported)
		}
		high, err := bptree.High(blk, page)
		if err != nil {
			return nil, fmt.Errorf("High: %w", err)
		}
		root.high = high
		root.page = page
	}
	{
		pageSize := blk.PageSize()
		maxOverflowSize := math.MaxUint32 * pageSize
		klen, vlen := bptree.InlineSize(pageSize, 5, maxOverflowSize, maxOverflowSize)
		root.klen = uint16(klen)
		root.vlen = uint16(vlen)
	}
	return root, nil
}

func (root *Root) High() uint8        { return root.high }
func (root *Root) Page() bptree.Page  { return root.page }
func (root *Root) KeyInlineSize() int { return int(root.klen) }
func (root *Root) ValInlineSize() int { return int(root.vlen) }
