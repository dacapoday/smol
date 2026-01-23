//go:build !debug

package heap

// assertBlockID is a no-op in production.
// Enable with -tags debug for runtime checks.
func assertBlockID(string, BlockID) {}

// assertEntrySize is a no-op in production.
// Enable with -tags debug for runtime checks.
func assertEntrySize(string, int, int) {}
