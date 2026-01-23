package heap

import (
	"github.com/dacapoday/smol"
)

var (
	ErrClosed             = smol.ErrClosed
	ErrReadOnly           = smol.ErrReadOnly
	ErrInvalidBlockSize   = smol.ErrInvalidBlockSize
	ErrInvalidCipherSuite = smol.ErrInvalidCipherSuite
	ErrInvalidCipherKey   = smol.ErrInvalidCipherKey
	ErrBadChecksum        = smol.ErrBadChecksum
	ErrBadMeta            = smol.ErrBadMeta
	ErrBadEntry           = smol.ErrBadEntry
	ErrBadFreelist        = smol.ErrBadFreelist
	ErrBadCipherSpec      = smol.ErrBadCipherSpec
	ErrUnknownMagicCode   = smol.ErrUnknownMagicCode
	ErrFileEmpty          = smol.ErrFileEmpty
	ErrFileTruncated      = smol.ErrFileTruncated
	ErrNoSpace            = smol.ErrNoSpace
	ErrUnsupported        = smol.ErrUnsupported
	errOutOfRange         = smol.ErrOutOfRange
)
