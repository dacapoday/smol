// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package heap

import (
	"math"
)

// block wraps file for fixed-size block I/O.
type block[F File] struct {
	file  F
	size  int64
	count uint32 // allocated blocks, also next BlockID
	limit uint32 // file capacity in blocks
}

func (block *block[F]) File() F {
	return block.file
}

func (block *block[F]) BlockSize() int {
	return int(block.size)
}

// blockCount must > 1
func (block *block[F]) load(file F, blockSize, blockCount uint32) {
	block.limit = blockCount
	block.count = blockCount
	block.size = int64(blockSize)
	block.file = file
}

func (block *block[F]) close() (err error) {
	block.limit = 0
	block.count = 0
	block.size = 0
	err = block.file.Close()
	var nilFile F
	block.file = nilFile
	return
}

func (block *block[F]) extend() (blockID BlockID, err error) {
	if block.count == 0 {
		err = ErrOutOfSpace
		return
	}
	if block.count >= block.limit {
		n := min(block.limit, 65536)
		if err = block.grow(n); err != nil {
			return
		}
	}
	blockID = BlockID(block.count)
	block.count++
	return
}

func (block *block[F]) grow(n uint32) (err error) {
	scale := min(int64(block.limit)+int64(n), int64(math.MaxUint32))
	if err = block.file.Truncate(scale * block.size); err == nil {
		block.limit = uint32(scale)
	}
	return
}

func (block *block[F]) readAt(buffer []byte, blockID BlockID) (int, error) {
	return block.file.ReadAt(buffer, int64(blockID)*block.size)
}

func (block *block[F]) writeAt(buffer []byte, blockID BlockID) (int, error) {
	return block.file.WriteAt(buffer, int64(blockID)*block.size)
}

func (block *block[F]) sync() error {
	return block.file.Sync()
}
