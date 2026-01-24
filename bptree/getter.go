// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package bptree

import (
	"bytes"

	"github.com/dacapoday/smol/overflow"
)

// Get retrieves the value for a key from the B+ tree at the given root snapshot.
// Returns nil if the key does not exist. The buf parameter can be used to reduce allocations.
func Get[B ReadOnly](block B, root Page, keyInlineSize, valInlineSize int, high uint8, buf, key []byte) (val []byte, err error) {
	var reader Reader[B]
	reader.Load(block, root, keyInlineSize, valInlineSize, high)
	defer reader.Close()

	if !reader.Seek(key) {
		err = reader.Error()
		return
	}

	if inlineKey := reader.InlineKey(); len(inlineKey) <= keyInlineSize {
		if !bytes.Equal(key, inlineKey) {
			return
		}
	} else {
		head, overflowSize, overflowID := Overflow(inlineKey, keyInlineSize)
		var cmp int
		cmp, err = overflow.Compare(reader.block, key, head, overflowSize, overflowID)
		if err != nil || cmp != 0 {
			return
		}
	}

	val = reader.ValCopy(buf)
	return
}
