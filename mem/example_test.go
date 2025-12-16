package mem_test

import (
	"fmt"

	"github.com/dacapoday/smol/mem"
)

func Example() {
	// No initialization needed - just declare and use
	var f mem.File

	// Write some data
	f.WriteAt([]byte("hello"), 0)
	f.WriteAt([]byte("world"), 5)

	// Read it back
	buf := make([]byte, 10)
	n, _ := f.ReadAt(buf, 0)
	fmt.Printf("%s\n", buf[:n])

	// Check file size
	fmt.Printf("Size: %d\n", f.Size())

	// Output:
	// helloworld
	// Size: 10
}
