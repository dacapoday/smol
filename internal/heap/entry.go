// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package heap

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

func checksum(data []byte) uint32 {
	return crc32.Checksum(data, castagnoliCrcTable)
}

func readEntry(read func([]byte) (int, error), entrySize int, rest []byte) (entry []byte, err error) {
	entrySize += 4
	buffer := make([]byte, entrySize+max(4, len(rest)))
	if _, err = read(buffer[:entrySize+4]); err != nil {
		return
	}
	if err = verifyEntry(buffer); err != nil {
		return
	}
	copy(buffer[entrySize:], rest)
	entry = buffer[4 : entrySize+len(rest)]
	return
}

func verifyEntry(entry []byte) error {
	if entry[1] == 0 && entry[0] == 0 {
		size := int(binary.LittleEndian.Uint16(entry[2:4])) + 4
		if size+4 <= len(entry) {
			if binary.LittleEndian.Uint32(entry[size:]) == checksum(entry[:size]) {
				return nil
			}
		}
	}
	return fmt.Errorf("%w entry", ErrInvalidMeta)
}

func writeEntry(write func([]byte) (int, error), entry, buffer []byte) (rest []byte, err error) {
	if overflow := len(buffer) - 8; len(entry) > overflow {
		rest = entry[overflow:]
		entry = entry[:overflow]
	}

	copy(buffer[4:], entry)
	binary.LittleEndian.PutUint16(buffer[2:], uint16(len(entry)))
	buffer[1] = 0
	buffer[0] = 0

	offset := 4 + len(entry)
	binary.LittleEndian.PutUint32(buffer[offset:], checksum(buffer[:offset]))

	_, err = write(buffer)
	return
}
