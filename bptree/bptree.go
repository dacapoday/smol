package bptree

type Root[C Checkpoint] struct {
	ckpt C
	page Page
	klen uint16
	vlen uint16
	high uint8
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
