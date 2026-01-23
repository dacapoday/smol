package heap

import (
	"encoding/binary"
	"fmt"
	"io"
)

func (codec *codec) loadEntry(r io.ReaderAt, meta *Meta) (err error) {
	var entry []byte
	if meta.EntryID < 2 {
		if len(meta.Entry) == 0 {
			return
		}
		entry = meta.Entry
	} else {
		offset := int64(meta.EntryID) * int64(meta.BlockSize)

		var head [4]byte
		if _, err = r.ReadAt(head[:], offset); err != nil {
			err = fmt.Errorf("read entry(%d): %w", meta.EntryID, err)
			return
		}
		if head[0] != 0 || head[1] != 0 {
			return fmt.Errorf("%w head", ErrBadEntry)
		}

		headSize := len(meta.Entry)
		size := headSize + int(binary.LittleEndian.Uint16(head[2:4]))
		if size > int(meta.BlockSize) {
			return fmt.Errorf("%w size", ErrBadEntry)
		}

		entry = make([]byte, size)
		if _, err = r.ReadAt(entry[headSize:], offset+4); err != nil {
			err = fmt.Errorf("read entry(%d): %w", meta.EntryID, err)
			return
		}
		copy(entry, meta.Entry)
	}

	if err = codec.decode(entry, 1); err != nil {
		return fmt.Errorf("%w: %w", ErrBadEntry, err)
	}

	meta.Entry = entry[:len(entry)-codec.size()]
	return
}

func loadPlainEntry(r io.ReaderAt, meta *Meta) (err error) {
	if meta.EntryID < 2 {
		return
	}

	offset := int64(meta.EntryID) * int64(meta.BlockSize)

	var head [4]byte
	if _, err = r.ReadAt(head[:], offset); err != nil {
		err = fmt.Errorf("read entry(%d): %w", meta.EntryID, err)
		return
	}
	if head[0] != 0 || head[1] != 0 {
		return fmt.Errorf("%w head", ErrBadEntry)
	}

	size := 4 + int(binary.LittleEndian.Uint16(head[2:4]))
	if size > int(meta.BlockSize) {
		return fmt.Errorf("%w size", ErrBadEntry)
	}

	entry := make([]byte, size+max(len(meta.Entry), 4))
	if _, err = r.ReadAt(entry[:size+4], offset); err != nil {
		err = fmt.Errorf("read entry(%d): %w", meta.EntryID, err)
		return
	}

	if binary.LittleEndian.Uint32(entry[size:]) != checksum(entry[:size]) {
		return fmt.Errorf("%w checksum", ErrBadEntry)
	}

	copy(entry[size:], meta.Entry)
	meta.Entry = entry[4 : size+len(meta.Entry)]
	return
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
	} else {
		meta.Entry = nil
	}

	copy(buffer[4:], entry)
	binary.LittleEndian.PutUint16(buffer[2:], uint16(len(entry)))
	buffer[1] = 0
	buffer[0] = 0

	_, err = heap.block.writeAt(buffer[:4+len(entry)], meta.EntryID)
	return
}

func (heap *Heap[F]) savePlainEntry(meta *Meta) (err error) {
	buffer := heap.buffer
	entry := meta.Entry
	if overflow := len(buffer) - 8; len(entry) > overflow {
		meta.Entry = entry[overflow:]
		entry = entry[:overflow]
	} else {
		meta.Entry = nil
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
