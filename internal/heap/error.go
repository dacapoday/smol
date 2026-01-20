package heap

import (
	"github.com/dacapoday/smol"
)

var (
	ErrOpened             = smol.ErrOpened
	ErrClosed             = smol.ErrClosed
	ErrUnsupported        = smol.ErrUnsupported
	ErrFileEmpty          = smol.ErrFileEmpty
	ErrFileTruncated      = smol.ErrFileTruncated
	ErrUnknownMagicCode   = smol.ErrUnknownMagicCode
	ErrInvalidBlockSize   = smol.ErrInvalidBlockSize
	ErrInvalidChecksum    = smol.ErrInvalidChecksum
	ErrInvalidCipherSuite = smol.ErrInvalidCipherSuite
	ErrInvalidCipherKey   = smol.ErrInvalidCipherKey
	ErrInvalidMeta        = smol.ErrInvalidMeta
	ErrInvalidFreelist    = smol.ErrInvalidFreelist
	ErrReadOnly           = smol.ErrReadOnly
	ErrOutOfRange         = smol.ErrOutOfRange
	ErrOutOfSpace         = smol.ErrOutOfSpace
)
