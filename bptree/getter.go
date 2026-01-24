// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package bptree

import (
	"bytes"
)

// Get retrieves the value for a key from the B+ tree at the given root snapshot.
// Returns nil if the key does not exist. The buf parameter can be used to reduce allocations.
func Get[B ReadOnly](block B, root Page, keyInlineSize, valInlineSize int, high uint8, buf, key []byte) (val []byte, err error) {
	reader := new(Reader[B])
	reader.Load(block, root, keyInlineSize, valInlineSize, high)

	if !reader.Seek(key) {
		err = reader.Error()
		return
	}

	if !bytes.Equal(reader.Key(), key) {
		return
	}

	val = reader.ValCopy(buf)
	return
}
