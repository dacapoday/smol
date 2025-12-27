// Package smol defines basic interfaces for building key-value database components.
package smol

import (
	"errors"
	"hash/crc32"
	"io"
)

// File provides access to a storage backend for the key-value database.
// The File interface is the minimum implementation required.
//
// The *os.File type satisfies this interface.
type File interface {
	io.ReaderAt
	io.WriterAt
	io.Closer

	// Truncate changes the size of the file.
	Truncate(size int64) error

	// Sync commits the current contents of the file to stable storage.
	// Typically, this means flushing the file system's in-memory copy
	// of recently written data to disk.
	Sync() error
}

// Block divides storage into fixed-size blocks indexed by BlockID.
// Requires copy-on-write data structures to work with Commit and Rollback.
type Block[C Checkpoint] interface {
	// Close releases the underlying storage.
	// Uncommitted changes are discarded.
	// All Checkpoints must be released before Close.
	// Accessing Block after Close causes undefined behavior.
	Close() error

	// Rollback discards all uncommitted changes.
	Rollback() error

	// Commit persists all changes atomically and returns a Checkpoint.
	// The entry is metadata persisted with the commit.
	Commit(entry []byte) (C, error)

	ReadWrite
}

// Checkpoint is reference to commit snapshot.
type Checkpoint interface {
	// Acquire increments the reference count.
	Acquire()

	// Release decrements the reference count.
	Release()
}

// BlockID identifies a block, starting from 2(0 and 1 are reserved).
type BlockID = uint32

// ReadWrite provides a set of methods for reading and writing blocks.
// It embeds ReadOnly and extends it with write operations and block allocation.
type ReadWrite interface {
	ReadOnly

	// PageSize returns the readable and writable range within a block.
	// Buffer read and write operations must stay within PageSize bytes.
	PageSize() int

	// AllocateBlock allocates a new block and returns its BlockID.
	// Blocks allocated by AllocateBlock should be recycled via RecycleBlock when no longer needed.
	//
	// Error Detection:
	// Check that returned BlockID > 1, otherwise an error has occurred.
	// Implementations may optionally provide Error() error method to return the error details.
	//
	// Example:
	//   blockID := block.AllocateBlock()
	//   if blockID < 2 {
	//     if err, ok := block.(interface{ Error() error }); ok {
	//       return err.Error()
	//     }
	//     return errors.New("allocation failed")
	//   }
	AllocateBlock() BlockID

	// RecycleBlock marks a BlockID as no longer needed, paired with AllocateBlock.
	// Recycling the same BlockID multiple times causes undefined behavior.
	RecycleBlock(BlockID)

	// WriteBlock writes buffer to storage at blockID.
	// Like io.WriterAt, but buffer may be modified after return (e.g., encryption).
	// Do not rely on buffer content after writing.
	WriteBlock(blockID BlockID, buffer []byte) error
}

// ReadOnly is a subset of Block interface for reading blocks.
// Provides buffer for block reading operations.
// All buffers from AllocateBuffer or LoadBlock must be recycled via RecycleBuffer.
type ReadOnly interface {
	// LoadBlock reads a block and returns its content buffer.
	// Equivalent to AllocateBuffer followed by ReadBlock, but the returned buffer
	// is read-only and must not be modified.
	// The buffer must be recycled via RecycleBuffer when no longer needed.
	LoadBlock(blockID BlockID) (buffer []byte, err error)

	// AllocateBuffer allocates a buffer for reading and writing blocks.
	// The buffer must be recycled via RecycleBuffer when no longer needed.
	AllocateBuffer() []byte

	// ReadBlock reads a block using buffer.
	// When reader is provided, access data via reader; otherwise data is copied to buffer.
	// Data received by reader is read-only and only valid during callback execution.
	ReadBlock(blockID BlockID, buffer []byte, reader func(block []byte)) error

	// RecycleBuffer ends the lifecycle of a buffer from AllocateBuffer or LoadBlock.
	// After recycling, do not hold references to the buffer or reuse it.
	// Recycling the same buffer multiple times causes undefined behavior.
	RecycleBuffer(buffer []byte)
}

var (
	ErrInvalidOverflowHead = errors.New("invalid OverflowHead")
	ErrInvalidOverflowPage = errors.New("invalid OverflowPage")
	ErrAllocateFailed      = errors.New("allocate BlockID failed")
)

var CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
