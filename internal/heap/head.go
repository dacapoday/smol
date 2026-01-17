package heap

import (
	"errors"
	"fmt"
	"io"
)

func (heap *Heap[F]) load(file F, opt Option) (meta *Meta, err error) {
	magic := opt.MagicCode()
	metaA, metaB, err := loadMeta(io.NewSectionReader(file, 0, 1<<17), magic)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return
		}

		if _, err = file.ReadAt([]byte{0}, 0); !errors.Is(err, io.EOF) {
			if err == nil {
				err = ErrFileTruncated
			}
			return
		}

		if opt.ReadOnly() {
			err = ErrFileEmpty
			return
		}

		return heap.init(file, opt)
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

	if meta.Version != 0 {
		meta = nil
		err = ErrUnsupported
		return
	}
	if meta.BlockCount > 2 {
		if _, err = file.ReadAt([]byte{0}, int64(meta.BlockCount-1)*int64(meta.BlockSize)-1); err != nil {
			meta = nil
			err = ErrFileTruncated
			return
		}
	}

	if err = heap.codec.load(file, opt, meta); err != nil {
		meta = nil
		return
	}

	heap.ckp = meta.Ckp
	heap.magic = magic
	heap.metaID = meta.ID
	heap.buffer = make([]byte, meta.BlockSize)
	heap.block.load(file, meta.BlockSize, meta.BlockCount)
	return
}

func loadMeta[RS io.ReadSeeker](f RS, magic [4]byte) (metaA, metaB *Meta, err error) {
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

func (heap *Heap[F]) init(file F, opt Option) (meta *Meta, err error) {
	meta0, err := heap.codec.init(file, opt)
	if err != nil {
		return
	}

	magic := opt.MagicCode()
	buffer := make([]byte, meta0.BlockSize)
	{
		buff := Buffer(buffer[:4])

		if err = encodeMeta(&buff, meta0); err != nil {
			return
		}

		buffer[3] = magic[3]
		buffer[2] = magic[2]
		buffer[1] = magic[1]
		buffer[0] = magic[0]

		if _, err = file.WriteAt(buff, 0); err != nil {
			return
		}

		if err = file.Sync(); err != nil {
			return
		}
	}

	meta = meta0

	heap.ckp = meta.Ckp
	heap.magic = magic
	heap.metaID = meta.ID
	heap.buffer = buffer
	heap.block.load(file, meta.BlockSize, meta.BlockCount)
	return
}

func (heap *Heap[F]) meta(blockID BlockID) (meta *Meta, err error) {
	return readMeta(heap.block.file, int64(blockID)*heap.block.size+4,
		heap.block.size-4)
}

func readMeta[R io.ReaderAt](r R, offset, limit int64) (meta *Meta, err error) {
	meta = new(Meta)
	if err = decodeMeta(io.NewSectionReader(r, offset, limit), meta); err != nil {
		meta = nil
	}
	return
}

func (heap *Heap[F]) save(meta *Meta) (err error) {
	meta.Freelist = heap.freelist()
	meta.FreeRecycled = heap.free.recycled
	meta.FreeTotal = heap.free.total
	meta.BlockCount = heap.block.count
	return heap.flush(meta)
}

func (heap *Heap[F]) flush(meta *Meta) (err error) {
	buffer := Buffer(heap.buffer[:4])

	if err = encodeMeta(&buffer, meta); err != nil {
		return
	}

	if meta.ID > 1 {
		buffer[3] = 0
		buffer[2] = 0
		buffer[1] = 0
		buffer[0] = 0

		if _, err = heap.block.writeAt(buffer, meta.ID); err != nil {
			return
		}
	}

	if err = heap.block.sync(); err != nil {
		return
	}

	buffer[3] = heap.magic[3]
	buffer[2] = heap.magic[2]
	buffer[1] = heap.magic[1]
	buffer[0] = heap.magic[0]

	if _, err = heap.block.writeAt(buffer, BlockID(meta.Ckp%2)); err != nil {
		return
	}

	return heap.block.sync()
}
