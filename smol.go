// Package smol defines basic interfaces for building key-value database components.
package smol

import (
	"io"
)

// File provides access to a storage backend.
// *os.File satisfies this interface.
type File interface {
	io.ReaderAt
	io.WriterAt
	io.Closer

	// Truncate changes the size of the file.
	Truncate(size int64) error

	// Sync commits file contents to stable storage.
	Sync() error
}

// Block divides storage into fixed-size blocks indexed by BlockID.
// Requires copy-on-write data structures for Commit and Rollback.
type Block[C Checkpoint] interface {
	// Close releases the underlying storage and discards uncommitted changes.
	// All Checkpoints must be released before Close.
	Close() error

	// Rollback discards all uncommitted changes.
	Rollback() error

	// Commit atomically persists all changes and returns a Checkpoint.
	// Entry is metadata persisted with the commit.
	//
	// Warning: Entry size must not exceed PageSize().
	Commit(entry []byte) (C, error)

	ReadWrite
}

// Checkpoint is a reference to a commit snapshot.
type Checkpoint interface {
	// Acquire increments the reference count.
	Acquire()

	// Release decrements the reference count.
	Release()
}

// BlockID identifies a block, starting from 2 (0 and 1 are reserved).
type BlockID = uint32

// ReadWrite provides methods for reading and writing blocks.
// Embeds ReadOnly and extends it with write operations.
type ReadWrite interface {
	ReadOnly

	// PageSize returns usable bytes within a block.
	PageSize() int

	// AllocateBlock allocates a new block and returns its BlockID.
	// Recycle via RecycleBlock when no longer needed.
	//
	// Error detection: If BlockID < 2, an error occurred.
	// Call Error() method if available for error details.

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

	// RecycleBlock marks a BlockID as no longer needed.
	// Recycling the same BlockID multiple times causes undefined behavior.
	RecycleBlock(BlockID)

	// WriteBlock writes buffer to storage at blockID.
	//
	// Warning: Caller must not rely on buffer content after writing as it may be modified.
	WriteBlock(blockID BlockID, buffer []byte) error
}

// ReadOnly is a subset of Block interface for reading blocks.
//
// Important: All buffers from AllocateBuffer or LoadBlock must be recycled via RecycleBuffer.
type ReadOnly interface {
	// LoadBlock reads a block and returns its content.
	// Returned buffer is read-only. Recycle via RecycleBuffer.
	LoadBlock(blockID BlockID) (buffer []byte, err error)

	// AllocateBuffer allocates a buffer for reading and writing blocks.
	// Recycle via RecycleBuffer when done.
	AllocateBuffer() []byte

	// ReadBlock reads a block using buffer.
	// If reader is provided, access data via reader; otherwise data is copied to buffer.
	//
	// Important: Buffer parameter is required even when reader is provided.
	ReadBlock(blockID BlockID, buffer []byte, reader func(block []byte)) error

	// NeedRecycleBuffer reports whether caller should recycle buffers
	// to relieve memory pressure. holding: buffers held by current session.
	//
	// Returns true when pool resources are strained. Callers may continue
	// allocating but should consider releasing buffers to reduce pressure.
	NeedRecycleBuffer(holding int) bool

	// RecycleBuffer releases a buffer from AllocateBuffer or LoadBlock.
	//
	// Warning: Caller must not use buffer after recycling.
	RecycleBuffer(buffer []byte)
}
