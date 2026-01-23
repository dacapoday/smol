//go:build debug

package heap

import "fmt"

// assertBlockID panics if blockID < 2.
// Only enabled with -tags debug.
func assertBlockID(method string, blockID BlockID) {
	if blockID < 2 {
		panic(fmt.Sprintf("%s: blockID %d < 2", method, blockID))
	}
}

// assertEntrySize panics if entry exceeds size limit.
// Only enabled with -tags debug.
func assertEntrySize(method string, entrySize, limit int) {
	if entrySize > limit {
		panic(fmt.Sprintf("%s: entrySize %d > %d", method, entrySize, limit))
	}
}
