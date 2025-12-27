package bptree

import (
	"bytes"
)

func Get[B ReadOnly, R RootBlock](block B, root R, key, buf []byte) (val []byte, err error) {
	reader := new(Reader[B, R])
	reader.Load(block, root)

	if !reader.Seek(key) {
		err = reader.Error()
		return
	}

	if !bytes.Equal(reader.Key(), key) {
		return
	}

	if val = append(buf[:0], reader.Val()...); val == nil {
		val = []byte{}
	}
	return
}
