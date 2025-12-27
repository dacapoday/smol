package heap

import (
	"github.com/dacapoday/smol"
)

var (
	ErrOpened           = smol.ErrOpened
	ErrClosed           = smol.ErrClosed
	ErrFileEmpty        = smol.ErrFileEmpty
	ErrFileTruncated    = smol.ErrFileTruncated
	ErrUnknownMagicCode = smol.ErrUnknownMagicCode
	ErrUnsupported      = smol.ErrUnsupported
	ErrInvalidBlockSize = smol.ErrInvalidBlockSize
	ErrInvalidMeta      = smol.ErrInvalidMeta
	ErrInvalidFreelist  = smol.ErrInvalidFreelist
	ErrReadOnly         = smol.ErrReadOnly
	ErrOutOfRange       = smol.ErrOutOfRange
	ErrOutOfSpace       = smol.ErrOutOfSpace
)
