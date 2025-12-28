// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package heap

import (
	"encoding/binary"
	"errors"
)

type Freelist []byte

func freelistSize(capacity uint16) int {
	return 4 + 4 + 4*int(capacity) + 4 // freePage(N)=head(4)+prev(4)+ids(4N)+crc(4)
}

func freelistCapacity(size int64) uint16 {
	return uint16((size - 4 - 4 - 4) / 4)
}

func freelist2ring(freelist Freelist, ring *ring, count uint16) {
	for i := range count {
		id := freelist.ID(i)
		if id < 2 {
			panic(errors.New("id < 2"))
		}
		if !ring.unshift(BlockID(id)) {
			panic(errors.New("out of ring"))
		}
	}
}

func ring2freelist(ring *ring, prev BlockID, freelist []byte) {
	binary.LittleEndian.PutUint32(freelist[4:], uint32(prev))
	binary.LittleEndian.PutUint16(freelist[2:], (ring.length+1)*4)
	freelist[1] = 0x40
	freelist[0] = 0
	var offset uint16
	for i, id := range ring.freelist {
		offset = 8 + i*4
		binary.LittleEndian.PutUint32(freelist[offset:], id)
	}
	offset = 8 + ring.length*4
	binary.LittleEndian.PutUint32(freelist[offset:], checksum(freelist[:offset]))
}

func (freelist Freelist) invalid() bool {
	if freelist[1] == 0x40 && freelist[0] == 0 {
		offset := 8 + freelist.Count()*4
		if binary.LittleEndian.Uint32(freelist[offset:]) == checksum(freelist[:offset]) {
			return false
		}
	}
	return true
}

func (freelist Freelist) Prev() BlockID {
	return BlockID(binary.LittleEndian.Uint32(freelist[4:]))
}

func (freelist Freelist) Count() uint16 {
	return binary.LittleEndian.Uint16(freelist[2:])/4 - 1
}

func (freelist Freelist) ID(index uint16) BlockID {
	return binary.LittleEndian.Uint32(freelist[8+index*4:])
}
