package heap

import (
	"encoding/binary"
	"fmt"
	"io"
)

func (codec *codec) loadEntry(r io.ReaderAt, meta *Meta) (err error) {
	size := int(meta.EntrySize) + codec.size()
	entrySize := size - len(meta.Entry)
	if entrySize == 0 {
		err = codec.decode(meta.Entry, 1)
		meta.Entry = meta.Entry[:meta.EntrySize]
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
	entrySize += 4
	entry := make([]byte, entrySize)
	if _, err = r.ReadAt(entry, int64(meta.EntryID)*int64(meta.BlockSize)); err != nil {
		err = fmt.Errorf("read entry(%d) failed: %w", meta.EntryID, err)
		return
	}
	if entry[1] == 0 && entry[0] == 0 {
		if int(binary.LittleEndian.Uint16(entry[2:4]))+4+codec.size() == len(entry) {
			entry = entry[4-len(meta.Entry):]
			copy(entry, meta.Entry)
			err = codec.decode(entry, 1)
			meta.Entry = entry[:meta.EntrySize]
			return
		}
	}
	return fmt.Errorf("%w entry", ErrInvalidMeta)
}

func loadPlainEntry(r io.ReaderAt, meta *Meta) (err error) {
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
	entrySize += 4
	entry := make([]byte, entrySize+max(4, len(meta.Entry)))
	if _, err = r.ReadAt(entry[:entrySize+4], int64(meta.EntryID)*int64(meta.BlockSize)); err != nil {
		err = fmt.Errorf("read entry(%d) failed: %w", meta.EntryID, err)
		return
	}
	if entry[1] == 0 && entry[0] == 0 {
		if int(binary.LittleEndian.Uint16(entry[2:4]))+8 == len(entry) {
			if binary.LittleEndian.Uint32(entry[entrySize:]) == checksum(entry[:entrySize]) {
				copy(entry[entrySize:], meta.Entry)
				meta.Entry = entry[4 : entrySize+len(meta.Entry)]
				return
			}
		}
	}
	return fmt.Errorf("%w entry", ErrInvalidMeta)
}

func (codec *codec) encodeEntry(entry []byte) []byte {
	if codec.spec == nil {
		return entry
	}

	encoded := make([]byte, len(entry)+codec.size())
	copy(encoded, entry)
	codec.encode(encoded, 1)
	return encoded
}

func (heap *Heap[F]) saveEntry(meta *Meta) (err error) {
	if heap.codec.spec == nil {
		return heap.savePlainEntry(meta)
	}

	buffer := heap.buffer
	entry := meta.Entry
	if overflow := len(buffer) - 4; len(entry) > overflow {
		offset := len(entry) - overflow
		meta.Entry = entry[:offset]
		entry = entry[offset:]
	}

	copy(buffer[4:], entry)
	binary.LittleEndian.PutUint16(buffer[2:], uint16(len(entry)-heap.codec.size()))
	buffer[1] = 0
	buffer[0] = 0

	_, err = heap.block.writeAt(buffer[:len(entry)+4], meta.EntryID)
	return
}

func (heap *Heap[F]) savePlainEntry(meta *Meta) (err error) {
	buffer := heap.buffer
	entry := meta.Entry
	if overflow := len(buffer) - 8; len(entry) > overflow {
		meta.Entry = entry[overflow:]
		entry = entry[:overflow]
	}

	copy(buffer[4:], entry)
	binary.LittleEndian.PutUint16(buffer[2:], uint16(len(entry)))
	buffer[1] = 0
	buffer[0] = 0

	offset := 4 + len(entry)
	binary.LittleEndian.PutUint32(buffer[offset:], checksum(buffer[:offset]))
	_, err = heap.block.writeAt(buffer[:offset+4], meta.EntryID)
	return
}
