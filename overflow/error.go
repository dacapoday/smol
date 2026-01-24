package overflow

import (
	"fmt"

	"github.com/dacapoday/smol"
)

var (
	ErrBadOverflow    = smol.ErrBadOverflow
	ErrAllocateFailed = smol.ErrAllocateFailed
)

func errAllocateFailed[B ReadWrite](b B) error {
	if block, ok := any(b).(interface{ Error() error }); ok {
		if err := block.Error(); err != nil {
			return fmt.Errorf("%w: %w", ErrAllocateFailed, err)
		}
	}
	return ErrAllocateFailed
}

func errOverflow(overflowSize int) error {
	if overflowSize < 0 {
		return fmt.Errorf("%w: %d bytes over", ErrBadOverflow, -overflowSize)
	}
	return fmt.Errorf("%w: %d bytes remaining", ErrBadOverflow, overflowSize)
}

func errNextID(nextID BlockID) error {
	return fmt.Errorf("%w: invalid nextID %d", ErrBadOverflow, nextID)
}
