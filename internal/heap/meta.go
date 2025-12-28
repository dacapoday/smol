// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package heap

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// Meta represents metadata of Heap that is serialized to and deserialized from
// file headers using TLV format. It contains essential information
// about blocks, transactions, free space management, and entry data.
type Meta struct {
	Entry    []byte   // Entry data content (key: 16)
	Freelist Freelist // Serialized freelist: linked list of BlockIDs in recycle order (key: 13)

	EntryID   BlockID // Entry data BlockID if Entry overflows Meta (key: 15)
	EntrySize uint32  // Size of entry data (key: 14)

	FreeTotal    uint32 // Total number of free blocks (not includes FreeRecycled) (key: 12)
	FreeRecycled uint32 // Number of blocks recycled since the current Checkpoint (key: 11)

	PrevID BlockID // Previous Meta's BlockID if RetainCheckpoints not zero (key: 10)
	ID     BlockID // Current Meta's BlockID if RetainCheckpoints not zero (key: 9)

	BlockCount uint32 // Total number of blocks (key: 8)
	BlockSize  uint32 // Size of each block in bytes (key: 7)

	UpdateTime int64  // Last update timestamp (key: 6)
	Ckp        uint32 // Checkpoint identifier (key: 5)

	Version byte // Version (key: 1)
}

func decodeMeta[R io.Reader](f R, meta *Meta) (err error) {
	c := crc32.New(castagnoliCrcTable)
	r := io.TeeReader(f, c)
	d := tlvDecoder{r}
	var key int64
	var val uint64
	for {
		key, err = d.readKey()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return
		}
		switch key {
		case 5:
			if val, err = d.readVal(); err != nil {
				return
			}
			meta.Ckp = uint32(val)
		case 6:
			if val, err = d.readVal(); err != nil {
				return
			}
			meta.UpdateTime = int64(val)
		case 7:
			if val, err = d.readVal(); err != nil {
				return
			}
			meta.BlockSize = uint32(val)
		case 8:
			if val, err = d.readVal(); err != nil {
				return
			}
			meta.BlockCount = uint32(val)
		case 11:
			if val, err = d.readVal(); err != nil {
				return
			}
			meta.FreeRecycled = uint32(val)
		case 12:
			if val, err = d.readVal(); err != nil {
				return
			}
			meta.FreeTotal = uint32(val)
		case 14:
			if val, err = d.readVal(); err != nil {
				return
			}
			meta.EntrySize = uint32(val)
		case 15:
			if val, err = d.readVal(); err != nil {
				return
			}
			meta.EntryID = BlockID(val)
		case 9:
			if val, err = d.readVal(); err != nil {
				return
			}
			meta.ID = BlockID(val)
		case 10:
			if val, err = d.readVal(); err != nil {
				return
			}
			meta.PrevID = BlockID(val)
		case -13:
			if val, err = d.readVal(); err != nil {
				return
			}
			val, err := d.readBytes(val)
			if err != nil {
				return err
			}
			meta.Freelist = Freelist(val)
		case -16:
			if val, err = d.readVal(); err != nil {
				return
			}
			val, err := d.readBytes(val)
			if err != nil {
				return err
			}
			meta.Entry = val
		case 1:
			if val, err = d.readVal(); err != nil {
				return
			}
			meta.Version = byte(val)
		case 0:
			var buf [4]byte
			if _, err = io.ReadFull(f, buf[:]); err != nil {
				if err == io.EOF {
					err = nil
				}
			} else {
				val := binary.LittleEndian.Uint32(buf[:])
				if c.Sum32() != val {
					err = fmt.Errorf("%w checksum", ErrInvalidMeta)
				}
			}
			return
		default:
			val, err = d.readVal()
			if err != nil {
				return
			}
			if key < 0 {
				if _, err = d.readBytes(val); err != nil {
					return
				}
			}
		}
	}
}

func encodeMeta[W io.Writer](f W, meta *Meta) (err error) {
	c := crc32.New(castagnoliCrcTable)
	w := io.MultiWriter(f, c)
	e := tlvEncoder{w}
	if err = e.writeBytes(16, meta.Entry); err != nil {
		return
	}
	if err = e.writeBytes(13, meta.Freelist); err != nil {
		return
	}
	if err = e.writeVal(5, uint64(meta.Ckp)); err != nil {
		return
	}
	if err = e.writeVal(6, uint64(meta.UpdateTime)); err != nil {
		return
	}
	if err = e.writeVal(7, uint64(meta.BlockSize)); err != nil {
		return
	}
	if err = e.writeVal(8, uint64(meta.BlockCount)); err != nil {
		return
	}
	if err = e.writeVal(9, uint64(meta.ID)); err != nil {
		return
	}
	if err = e.writeVal(10, uint64(meta.PrevID)); err != nil {
		return
	}
	if err = e.writeVal(11, uint64(meta.FreeRecycled)); err != nil {
		return
	}
	if err = e.writeVal(12, uint64(meta.FreeTotal)); err != nil {
		return
	}
	if err = e.writeVal(14, uint64(meta.EntrySize)); err != nil {
		return
	}
	if err = e.writeVal(15, uint64(meta.EntryID)); err != nil {
		return
	}
	{
		var buf [4]byte
		if _, err = e.Write(buf[:1]); err != nil {
			return
		}
		binary.LittleEndian.PutUint32(buf[:], uint32(c.Sum32()))
		_, err = f.Write(buf[:])
	}
	return
}

// tlvDecoder helps read TLV encoded data
type tlvDecoder struct {
	io.Reader
}

func (d tlvDecoder) ReadByte() (byte, error) {
	var buf [1]byte
	_, err := d.Read(buf[:])
	return buf[0], err
}

func (d tlvDecoder) readVal() (uint64, error) {
	return binary.ReadUvarint(d)
}

func (d tlvDecoder) readKey() (int64, error) {
	return binary.ReadVarint(d)
}

func (d tlvDecoder) readBytes(length uint64) (bytes []byte, err error) {
	if length >= 1<<16 {
		err = fmt.Errorf("%w bytes", ErrInvalidMeta)
		return
	}

	bytes = make([]byte, length)
	_, err = io.ReadFull(d, bytes)
	return
}

// tlvEncoder helps write TLV encoded data
type tlvEncoder struct {
	io.Writer
}

func (e tlvEncoder) writeVal(key int64, val uint64) (err error) {
	if val == 0 {
		return
	}

	var buf [binary.MaxVarintLen64]byte

	n := binary.PutVarint(buf[:], key)
	if _, err = e.Write(buf[:n]); err != nil {
		return
	}

	n = binary.PutUvarint(buf[:], val)
	_, err = e.Write(buf[:n])
	return
}

func (e tlvEncoder) writeBytes(key int64, val []byte) (err error) {
	if val == nil {
		return
	}

	var buf [binary.MaxVarintLen64]byte

	n := binary.PutVarint(buf[:], -key)
	if _, err = e.Write(buf[:n]); err != nil {
		return
	}

	n = binary.PutUvarint(buf[:], uint64(len(val)))
	if _, err = e.Write(buf[:n]); err != nil {
		return
	}

	_, err = e.Write(val)
	return
}
