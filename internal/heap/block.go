// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package heap

import (
	"errors"
	"fmt"
	"io"
	"math"
	"time"
)

type block[F File] struct {
	file   F
	buffer []byte
	size   int64
	limit  uint32
	count  uint32
	magic  [4]byte
}

func (block *block[F]) File() F {
	return block.file
}

// func (block *block[F]) BlockSize() int {
// 	return int(block.size)
// }

func (block *block[F]) init(file F, magic [4]byte, blockSize uint32) (meta *Meta, err error) {
	meta0 := &Meta{
		BlockSize:  blockSize,
		BlockCount: 2,
		UpdateTime: time.Now().UnixMilli(),
	}

	buff := make([]byte, blockSize)
	{
		buffer := Buffer(buff[:4])

		if err = encodeMeta(&buffer, meta0); err != nil {
			return
		}

		buffer[3] = magic[3]
		buffer[2] = magic[2]
		buffer[1] = magic[1]
		buffer[0] = magic[0]

		if _, err = file.WriteAt(buffer, 0); err != nil {
			return
		}
	}

	if err = file.Sync(); err != nil {
		return
	}

	meta = meta0

	block.magic = magic
	block.size = int64(blockSize)
	block.limit = 2
	block.count = 2
	block.file = file
	block.buffer = buff
	return
}

func (block *block[F]) load(file F, magic [4]byte) (meta *Meta, err error) {
	metaA, metaB, err := load(io.NewSectionReader(file, 0, 1<<17), magic)
	if err != nil {
		if errors.Is(err, io.EOF) {
			if _, err = file.ReadAt([]byte{0}, 0); errors.Is(err, io.EOF) {
				err = ErrFileEmpty
			}
		}
		return
	}

	if metaA != nil && metaB != nil {
		if metaA.Ckp < metaB.Ckp {
			if metaA.Ckp == 0 && metaA.Ckp-1 == metaB.Ckp {
				meta = metaA
			} else {
				meta = metaB
			}
		} else {
			meta = metaA
		}
	} else if metaA != nil {
		meta = metaA
	} else if metaB != nil {
		meta = metaB
	} else {
		panic(errors.New("metaA == nil && metaB == nil"))
	}

	blockSize := int64(meta.BlockSize)
	if meta.BlockCount > 2 {
		if _, err = file.ReadAt([]byte{0}, int64(meta.BlockCount-1)*blockSize-1); err != nil {
			meta = nil
			err = ErrFileTruncated
			return
		}
	}

	block.magic = magic
	block.size = blockSize
	block.limit = meta.BlockCount
	block.count = meta.BlockCount
	block.file = file
	block.buffer = make([]byte, block.size)
	return
}

func load[RS io.ReadSeeker](f RS, magic [4]byte) (metaA, metaB *Meta, err error) {
	load := func(offset int64) (meta *Meta, err error) {
		if _, err = f.Seek(offset, io.SeekStart); err != nil {
			return
		}

		var head [4]byte
		if _, err = f.Read(head[:]); err != nil {
			return
		}
		if head != magic {
			err = fmt.Errorf("%w %v", ErrUnknownMagicCode, head)
			return
		}

		meta = new(Meta)
		if err = decodeMeta(f, meta); err != nil {
			meta = nil
		} else if meta.Version != 0 {
			meta = nil
			err = ErrUnsupported
		}
		return
	}

	metaA, err = load(0)
	if err == nil {
		metaB, _ = load(int64(metaA.BlockSize))
		return
	}

	if errors.Is(err, ErrUnknownMagicCode) {
		err = fmt.Errorf("metaA has %w", err)
		return
	}

	for i := range 5 {
		offset := int64(4096) << i
		if metaB, err = load(offset); err == nil {
			return
		}
		if !errors.Is(err, ErrUnknownMagicCode) {
			err = fmt.Errorf("metaB: %w", err)
			return
		}
	}
	err = fmt.Errorf("metaB has %w", err)
	return
}

func (block *block[F]) opened() bool {
	return block.buffer != nil
}

func (block *block[F]) close() (err error) {
	block.limit = 0
	block.size = 0
	block.buffer = nil
	err = block.file.Close()
	var nilFile F
	block.file = nilFile
	return
}

func (block *block[F]) meta(blockID BlockID) (meta *Meta, err error) {
	meta = new(Meta)
	err = decodeMeta(io.NewSectionReader(block.file, int64(blockID)*block.size+4, block.size-4), meta)
	if err != nil {
		meta = nil
	}
	return
}

func (block *block[F]) loadEntry(meta *Meta) (err error) {
	entrySize := int(meta.EntrySize) - len(meta.Entry)
	if entrySize == 0 {
		return
	}
	if entrySize < 0 {
		err = fmt.Errorf("%w entrySize", ErrInvalidMeta)
		return
	}
	if meta.EntryID < 2 {
		err = fmt.Errorf("%w entryID", ErrInvalidMeta)
		return
	}
	meta.Entry, err = block.readEntry(meta.EntryID, entrySize, meta.Entry)
	if err != nil {
		err = fmt.Errorf("read entry(%d) failed: %w", meta.EntryID, err)
		return
	}
	return
}

func (block *block[F]) readEntry(entryID BlockID, entrySize int, front []byte) ([]byte, error) {
	return readEntry(
		func(buffer []byte) (int, error) {
			return block.readAt(buffer, entryID)
		},
		entrySize, front)
}

func (block *block[F]) writeEntry(entryID BlockID, entry []byte) ([]byte, error) {
	return writeEntry(
		func(buffer []byte) (int, error) {
			return block.writeAt(buffer, entryID)
		},
		entry, block.buffer)
}

func (block *block[F]) save(meta *Meta) (err error) {
	buffer := Buffer(block.buffer[:4])

	if err = encodeMeta(&buffer, meta); err != nil {
		fmt.Println(meta)
		return
	}

	if meta.ID > 1 {
		buffer[3] = 0
		buffer[2] = 0
		buffer[1] = 0
		buffer[0] = 0

		if _, err = block.writeAt(buffer, meta.ID); err != nil {
			return
		}
	}

	if err = block.sync(); err != nil {
		return
	}

	buffer[3] = block.magic[3]
	buffer[2] = block.magic[2]
	buffer[1] = block.magic[1]
	buffer[0] = block.magic[0]

	if _, err = block.writeAt(buffer, BlockID(meta.Ckp%2)); err != nil {
		return
	}

	return block.sync()
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

// Buffer is a byte slice wrapper that implements the io.Writer interface.
// It provides a way to write data to a slice using the standard Write method,
// The buffer uses the slice's capacity as the maximum size limit and length as the current write position marker.
type Buffer []byte

var _ io.Writer = (*Buffer)(nil)

// Write appends data to the buffer
// Returns the number of bytes written and an error if the write would exceed the buffer's capacity.
func (buffer *Buffer) Write(p []byte) (n int, err error) {
	b := *buffer
	n = len(p)
	if n+len(b) > cap(b) {
		return 0, ErrOutOfRange
	}
	*buffer = append(b, p...)
	return
}
