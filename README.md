smol
====

[![Go Reference](https://pkg.go.dev/badge/github.com/dacapoday/smol/kv.svg)](https://pkg.go.dev/github.com/dacapoday/smol/kv)

A key-value store written in Go, based on copy-on-write B+ tree.

## Status

Under active development. The `kv` package is ready for use.

## kv Package

Disk-based key-value store with:

- **Ordered Keys**: Lexicographic order via copy-on-write B+ tree
- **MVCC**: Concurrent reads and writes with snapshot isolation
- **Transactions**: Read Committed isolation with rollback support
- **File Size**: 32 KiB minimum, 64 TiB theoretical maximum
- **Key/Value Size**: No hard limit (recommended: keys < 3258 bytes, values < 13092 bytes)

File format specification is defined in the `ksy/` directory, visualizable with [Kaitai Struct](https://kaitai.io/).

## Installation

```bash
go get github.com/dacapoday/smol/kv
```

## Get Started

```go
package main

import (
    "fmt"
    "log"

    "github.com/dacapoday/smol/kv"
)

func main() {
    // Open or create database file
    db, err := kv.Open("my-data.kv")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Write
    err = db.Set([]byte("hello"), []byte("world"))
    if err != nil {
        log.Fatal(err)
    }

    // Read
    value, err := db.Get([]byte("hello"))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("hello: %s\n", value)

    // Delete (set value to nil)
    err = db.Set([]byte("hello"), nil)
    if err != nil {
        log.Fatal(err)
    }
}
```

### Transactions

```go
tx := db.Begin()

// Multiple changes
tx.Set([]byte("user:1:name"), []byte("dacapoday"))
tx.Set([]byte("user:1:email"), []byte("dacapoday@gmail.com"))

// Read (sees uncommitted changes)
name, _ := tx.Get([]byte("user:1:name"))
fmt.Printf("Name: %s\n", name)

// Commit atomically
err = tx.Commit()
if err != nil {
    log.Fatal(err)
}

// Or rollback to discard changes
```

## File Format

Database file format visualized with Kaitai Struct IDE:

![Kaitai Struct IDE Screenshot](doc/ksy_ide_screen.png)
