// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package bptree

import "github.com/dacapoday/smol/overflow"

// KeyStr returns the current key as string, or empty if invalid.
func (reader *Reader[B, R]) KeyStr() string {
	return b2s(reader.Key())
}

// ValStr returns the current value as string, or empty if invalid.
func (reader *Reader[B, R]) ValStr() string {
	return b2s(reader.Val())
}

// KeyCopy copies the current key to buf and returns the result.
// Uses buf directly for overflow reads to avoid intermediate allocation.
// Returns nil if reader is invalid, []byte{} if key is empty.
func (reader *Reader[B, R]) KeyCopy(buf []byte) (key []byte) {
	if reader.err != null {
		return
	}
	k := reader.page.LeafKey(reader.index)
	if len(k) <= reader.root.KeyInlineSize() {
		key = append(buf[:0], k...)
	} else if len(reader.key) != 0 {
		key = append(buf[:0], reader.key...)
	} else {
		var err error
		key, err = overflow.Read(reader.block, buf, k)
		if err != nil {
			reader.err = err
			return
		}
	}
	if key == nil {
		key = []byte{}
	}
	return
}

// ValCopy copies the current value to buf and returns the result.
// Uses buf directly for overflow reads to avoid intermediate allocation.
// Returns nil if reader is invalid, []byte{} if value is empty.
func (reader *Reader[B, R]) ValCopy(buf []byte) (val []byte) {
	if reader.err != null {
		return
	}
	v := reader.page.LeafVal(reader.index)
	if len(v) <= reader.root.ValInlineSize() {
		val = append(buf[:0], v...)
	} else if len(reader.val) != 0 {
		val = append(buf[:0], reader.val...)
	} else {
		var err error
		val, err = overflow.Read(reader.block, buf, v)
		if err != nil {
			reader.err = err
			return
		}
	}
	if val == nil {
		val = []byte{}
	}
	return
}

// InlineKey returns the key bytes stored directly in the page slot.
// For keys within inline size limit, this equals Key().
// For overflow keys, returns the inline portion (overflow reference).
//
// Warning: Returned slice is valid only until next method call.
func (reader *Reader[B, R]) InlineKey() (key []byte) {
	if reader.err != null {
		return
	}
	key = reader.page.LeafKey(reader.index)
	return
}

// InlineVal returns the value bytes stored directly in the page slot.
// For values within inline size limit, this equals Val().
// For overflow values, returns the inline portion (overflow reference).
//
// Warning: Returned slice is valid only until next method call.
func (reader *Reader[B, R]) InlineVal() (val []byte) {
	if reader.err != null {
		return
	}
	val = reader.page.LeafVal(reader.index)
	return
}

// InlineKeyStr returns the inline key as string.
func (reader *Reader[B, R]) InlineKeyStr() string {
	return b2s(reader.InlineKey())
}

// InlineValStr returns the inline value as string.
func (reader *Reader[B, R]) InlineValStr() string {
	return b2s(reader.InlineVal())
}

// InlineKeyCopy appends the inline key to buf and returns the result.
func (reader *Reader[B, R]) InlineKeyCopy(buf []byte) []byte {
	return append(buf[:0], reader.InlineKey()...)
}

// InlineValCopy appends the inline value to buf and returns the result.
func (reader *Reader[B, R]) InlineValCopy(buf []byte) []byte {
	return append(buf[:0], reader.InlineVal()...)
}
