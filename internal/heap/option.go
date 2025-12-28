// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package heap

type Option interface {
	MagicCode() [4]byte
	ReadOnly() bool
	IgnoreInvalidFreelist() bool
	RetainCheckpoints() uint8
}

type BlockSize interface {
	BlockSize() int
}
