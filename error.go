package smol

import "errors"

var (
	ErrClosed             = errors.New("closed")
	ErrReadOnly           = errors.New("read-only")
	ErrInvalidBlockSize   = errors.New("invalid block size")
	ErrInvalidCipherSuite = errors.New("invalid cipher suite")
	ErrInvalidCipherKey   = errors.New("invalid cipher key")
	ErrBadChecksum        = errors.New("bad checksum")
	ErrBadMeta            = errors.New("bad meta")
	ErrBadEntry           = errors.New("bad entry")
	ErrBadFreelist        = errors.New("bad freelist")
	ErrBadOverflow        = errors.New("bad overflow")
	ErrBadCipherSpec      = errors.New("bad cipher spec")
	ErrUnknownMagicCode   = errors.New("unknown magic code")
	ErrFileEmpty          = errors.New("empty file")
	ErrFileTruncated      = errors.New("file truncated")
	ErrNoSpace            = errors.New("no space")
	ErrUnsupported        = errors.New("unsupported")
	ErrOutOfRange         = errors.New("out of range")
	ErrAllocateFailed     = errors.New("allocate failed")
)
