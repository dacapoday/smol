// Package smol defines basic interfaces for building key-value database components.
package smol

import "io"

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
