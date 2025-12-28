// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package bptree

import (
	"bytes"
)

// Get retrieves the value for a key from the B+ tree at the given root snapshot.
// Returns nil if the key does not exist. The buf parameter can be used to reduce allocations.
func Get[B ReadOnly, R RootBlock](block B, root R, buf, key []byte) (val []byte, err error) {
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
