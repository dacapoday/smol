package bptree

import (
	"errors"
	"fmt"

	"github.com/dacapoday/smol"
)

var (
	ErrClosed         = smol.ErrClosed
	ErrUnsupported    = smol.ErrUnsupported
	ErrAllocateFailed = smol.ErrAllocateFailed
)

var null = errors.New("")
var exhausted = errors.New("exhausted")

func errAllocateFailed[B ReadWrite](b B) error {
	if block, ok := any(b).(interface{ Error() error }); ok {
		if err := block.Error(); err != nil {
			return fmt.Errorf("%w: %w", ErrAllocateFailed, err)
		}
	}
	return ErrAllocateFailed
}
