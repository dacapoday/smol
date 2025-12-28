package kv_test

import (
	"fmt"
	"os"

	"github.com/dacapoday/smol/kv"
)

func Example() {
	// Create temporary file for demo
	var path string
	{
		f, err := os.CreateTemp("", "example-*.kv")
		if err != nil {
			panic(err)
		}
		path = f.Name()
		f.Close()
	}

	// Open creates or opens a database file
	db, err := kv.Open(path)
	if err != nil {
		panic(err)
	}

	// Set a key-value pair
	db.Set([]byte("hello"), []byte("world"))

	// Get the value for a key
	hello, _ := db.Get([]byte("hello"))
	fmt.Printf("hello: %s\n", hello)

	// Delete by setting value to nil
	db.Set([]byte("hello"), nil)

	// Close releases resources
	db.Close()

	// Output:
	// hello: world
}

func ExampleKV_Batch() {
	// Create temporary file for demo
	var path string
	{
		f, err := os.CreateTemp("", "example-*.kv")
		if err != nil {
			panic(err)
		}
		path = f.Name()
		f.Close()
	}

	db, err := kv.Open(path)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Batch writes multiple key-value pairs atomically
	err = db.Batch(func(yield func([]byte, []byte) bool) {
		if !yield([]byte("4"), []byte("Mars")) {
			return
		}
		if !yield([]byte("2"), []byte("Venus")) {
			return
		}
		if !yield([]byte("3"), []byte("Earth")) {
			return
		}
	})
	if err != nil {
		panic(err)
	}

	// Verify data was written in sorted order
	second, _ := db.Get([]byte("2"))
	third, _ := db.Get([]byte("3"))
	fourth, _ := db.Get([]byte("4"))

	fmt.Printf("2nd planet: %s\n", second)
	fmt.Printf("3rd planet: %s\n", third)
	fmt.Printf("4th planet: %s\n", fourth)

	// Output:
	// 2nd planet: Venus
	// 3rd planet: Earth
	// 4th planet: Mars
}

func ExampleKV_Iter() {
	// Create temporary file for demo
	var path string
	{
		f, err := os.CreateTemp("", "example-*.kv")
		if err != nil {
			panic(err)
		}
		path = f.Name()
		f.Close()
	}

	db, err := kv.Open(path)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Insert some planets
	db.Set([]byte("Mars"), []byte("Red Planet"))
	db.Set([]byte("Jupiter"), []byte("Gas Giant"))
	db.Set([]byte("Saturn"), []byte("Ringed"))

	// Create iterator (captures a snapshot)
	iter := db.Iter()
	defer iter.Close() // Important: always close iterator

	// Iterate in sorted order
	for iter.SeekFirst(); iter.Valid(); iter.Next() {
		fmt.Printf("%s: %s\n", iter.Key(), iter.Val())
	}

	// Output:
	// Jupiter: Gas Giant
	// Mars: Red Planet
	// Saturn: Ringed
}

func ExampleKV_Begin() {
	// Create temporary file for demo
	var path string
	{
		f, err := os.CreateTemp("", "example-*.kv")
		if err != nil {
			panic(err)
		}
		path = f.Name()
		f.Close()
	}

	db, err := kv.Open(path)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Initial data
	db.Set([]byte("status"), []byte("idle"))

	// Transaction 1: Rollback
	tx1 := db.Begin()
	tx1.Set([]byte("status"), []byte("launching"))
	tx1.Rollback() // Discard changes

	status, _ := db.Get([]byte("status"))
	fmt.Printf("after rollback: %s\n", status)

	// Transaction 2: Commit
	tx2 := db.Begin()
	tx2.Set([]byte("status"), []byte("orbit"))
	tx2.Set([]byte("location"), []byte("Moon"))
	tx2.Commit() // Apply changes atomically

	status, _ = db.Get([]byte("status"))
	location, _ := db.Get([]byte("location"))
	fmt.Printf("after commit: %s at %s\n", status, location)

	// Output:
	// after rollback: idle
	// after commit: orbit at Moon
}

func ExampleTx_Iter() {
	// Create temporary file for demo
	var path string
	{
		f, err := os.CreateTemp("", "example-*.kv")
		if err != nil {
			panic(err)
		}
		path = f.Name()
		f.Close()
	}

	db, err := kv.Open(path)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Initialize planets
	db.Set([]byte("Earth"), []byte("home"))
	db.Set([]byte("Mars"), []byte("target"))

	// Begin transaction - Read Committed isolation
	tx := db.Begin()
	tx.Set([]byte("Mars"), []byte("reached"))
	tx.Set([]byte("Moon"), []byte("passed"))

	// Transaction iterator sees uncommitted changes
	iter := tx.Iter()
	defer iter.Close()
	fmt.Println("in transaction:")
	for iter.SeekFirst(); iter.Valid(); iter.Next() {
		fmt.Printf("  %s: %s\n", iter.Key(), iter.Val())
	}

	// Rollback discards all changes
	tx.Rollback()

	mars, _ := db.Get([]byte("Mars"))
	moon, _ := db.Get([]byte("Moon"))
	fmt.Printf("after rollback: Mars=%s, Moon=%v\n", mars, moon)

	// Output:
	// in transaction:
	//   Earth: home
	//   Mars: reached
	//   Moon: passed
	// after rollback: Mars=target, Moon=[]
}
