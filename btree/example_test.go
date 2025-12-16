package btree

import "fmt"

func Example() {
	var btree BTree

	// Set some key-value pairs
	btree.Set([]byte("apple"), []byte("red"))
	btree.Set([]byte("banana"), []byte("yellow"))
	btree.Set([]byte("cherry"), []byte("red"))

	// Get a value
	val, found := btree.Get([]byte("banana"))
	fmt.Printf("banana: %s (found: %v)\n", val, found)

	// Check if empty
	fmt.Printf("Empty: %v\n", btree.Empty())

	// Soft delete
	btree.Set([]byte("banana"), nil)
	val, found = btree.Get([]byte("banana"))
	fmt.Printf("banana after delete: %v (found: %v)\n", val, found)

	// Reset clears all data
	btree.Reset()
	fmt.Printf("Empty after reset: %v\n", btree.Empty())

	// Output:
	// banana: yellow (found: true)
	// Empty: false
	// banana after delete: [] (found: true)
	// Empty after reset: true
}

func ExampleBTree_Iter() {
	var btree BTree
	btree.Set([]byte("apple"), []byte("red"))
	btree.Set([]byte("banana"), []byte("yellow"))
	btree.Set([]byte("cherry"), []byte("red"))

	// Forward iteration
	iter := btree.Iter()
	iter.SeekFirst()
	for iter.Valid() {
		fmt.Printf("%s: %s\n", iter.Key(), iter.Val())
		iter.Next()
	}

	// Output:
	// apple: red
	// banana: yellow
	// cherry: red
}

func ExampleBTree_Items() {
	var btree BTree
	btree.Set([]byte("apple"), []byte("red"))
	btree.Set([]byte("banana"), nil) // deleted
	btree.Set([]byte("cherry"), []byte("red"))

	// Iterate using range
	for key, val := range btree.Items {
		if val == nil {
			fmt.Printf("%s: <deleted>\n", key)
		} else {
			fmt.Printf("%s: %s\n", key, val)
		}
	}

	// Output:
	// apple: red
	// banana: <deleted>
	// cherry: red
}
