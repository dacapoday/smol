package smol

import "errors"

var (
	ErrOpened              = errors.New("opened")
	ErrClosed              = errors.New("closed")
	ErrUnsupported         = errors.New("unsupported")
	ErrFileEmpty           = errors.New("empty file")
	ErrFileTruncated       = errors.New("file truncated")
	ErrUnknownMagicCode    = errors.New("unknown magic code")
	ErrInvalidBlockSize    = errors.New("invalid block size")
	ErrInvalidChecksum     = errors.New("invalid checksum")
	ErrInvalidCipherSuite  = errors.New("invalid cipher suite")
	ErrInvalidCipherKey    = errors.New("invalid cipher key")
	ErrInvalidMeta         = errors.New("invalid meta")
	ErrInvalidFreelist     = errors.New("invalid freelist")
	ErrReadOnly            = errors.New("read only")
	ErrOutOfRange          = errors.New("out of range")
	ErrOutOfSpace          = errors.New("out of space")
	ErrInvalidOverflowHead = errors.New("invalid OverflowHead")
	ErrInvalidOverflowPage = errors.New("invalid OverflowPage")
	ErrAllocateFailed      = errors.New("allocate BlockID failed")
)
