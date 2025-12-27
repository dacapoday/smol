package bptree

type Reader[B ReadOnly, R RootBlock] struct {
	block    B
	root     R
	err      error
	level    Level
	page     Page   // buf
	key, val []byte // buf
	count    uint16
	index    uint16
}

func (reader *Reader[B, R]) Block() B {
	return reader.block
}

func (reader *Reader[B, R]) Root() R {
	return reader.root
}

func (reader *Reader[B, R]) Load(block B, root R) {
	reader.block = block
	reader.root = root
	reader.err = exhausted
	// if len(reader.level) != 0 {
	// 	reader.block.RecycleBuffer(reader.page)
	// }
	if high := root.High(); high == 0 {
		reader.level = nil
		reader.page = root.Page()
	} else {
		reader.level = make(Level, high)
		reader.page = block.AllocateBuffer()
	}
	reader.count = 0
	reader.index = 0
}

func (dst *Reader[B, R]) Clone(src *Reader[B, R]) {
	dst.block = src.block
	dst.root = src.root
	dst.err = src.err
	dst.count = src.count
	dst.index = src.index
	dst.key = nil
	dst.val = nil
	if dst.err == nil {
		dst.level = nil
		dst.page = nil
		return
	}
	if len(src.level) == 0 {
		dst.level = nil
		dst.page = src.page
		return
	}
	dst.level = append(Level(nil), src.level...)
	dst.page = src.block.AllocateBuffer()
	if dst.err != null {
		return
	}
	if err := dst.block.ReadBlock(dst.level[0].BlockID, dst.page, nil); err != nil {
		dst.err = err
	}
}

func (reader *Reader[B, R]) Close() {
	if len(reader.level) != 0 {
		reader.block.RecycleBuffer(reader.page)
	}

	reader.err = nil
	reader.level = nil
	reader.page = nil
	reader.key = nil
	reader.val = nil
	reader.count = 0
	reader.index = 0

	var nilBlock B
	reader.block = nilBlock
	var nilRoot R
	reader.root = nilRoot
}

func (reader *Reader[B, R]) Level() Level {
	return append(Level(nil), reader.level...)
}

func (reader *Reader[B, R]) next() bool {
	reader.index++
	if reader.index < reader.count {
		return true
	}
	reader.index--
	return false
}

func (reader *Reader[B, R]) prev() bool {
	if reader.index == 0 {
		return false
	}
	reader.index--
	return true
}
