// Package block provides implementations of block interfaces.
package block

import (
	"hash/crc32"

	"github.com/dacapoday/smol"
	"github.com/dacapoday/smol/internal/heap"
)

type File = heap.File

type HeapCheckpoint = heap.Checkpoint

type BlockID = heap.BlockID

type HeapOption interface {
	MagicCode() [4]byte
	ReadOnly() bool
	IgnoreInvalidFreelist() bool
	RetainCheckpoints() uint8
}

func checksum(data []byte) uint32 {
	return crc32.Checksum(data, smol.CastagnoliCrcTable)
}

var _ smol.Block[HeapCheckpoint] = (*CRC32Heap[File])(nil)
