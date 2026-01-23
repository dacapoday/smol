// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package heap

import "io"

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
		return 0, errOutOfRange
	}
	*buffer = append(b, p...)
	return
}
