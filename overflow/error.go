package overflow

import (
	"fmt"

	"github.com/dacapoday/smol"
)

var (
	ErrInvalidOverflowHead = smol.ErrInvalidOverflowHead
	ErrInvalidOverflowPage = smol.ErrInvalidOverflowPage
	ErrAllocateFailed      = smol.ErrAllocateFailed
)

func errAllocateFailed[B ReadWrite](b B) error {
	if block, ok := any(b).(interface{ Error() error }); ok {
		if err := block.Error(); err != nil {
			return fmt.Errorf("%w: %w", ErrAllocateFailed, err)
		}
	}
	return ErrAllocateFailed
}
