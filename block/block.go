// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

// Package block provides implementations of block interfaces.
package block

import (
	"hash/crc32"

	"github.com/dacapoday/smol"
	"github.com/dacapoday/smol/internal/heap"
)

type File = smol.File
type BlockID = smol.BlockID
type HeapCheckpoint = heap.Checkpoint

type HeapOption interface {
	MagicCode() [4]byte
	ReadOnly() bool
	IgnoreInvalidFreelist() bool
	RetainCheckpoints() uint8
}

var castagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)

func checksum(data []byte) uint32 {
	return crc32.Checksum(data, castagnoliCrcTable)
}

var _ smol.Block[HeapCheckpoint] = (*CRC32Heap[File])(nil)
