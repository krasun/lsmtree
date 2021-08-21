# lsmtree

[![Build Status](https://travis-ci.com/krasun/lsmtree.svg?branch=main)](https://travis-ci.com/krasun/lsmtree)
[![codecov](https://codecov.io/gh/krasun/lsmtree/branch/main/graph/badge.svg?token=8NU6LR4FQD)](https://codecov.io/gh/krasun/lsmtree)
[![Go Report Card](https://goreportcard.com/badge/github.com/krasun/lsmtree)](https://goreportcard.com/report/github.com/krasun/lsmtree)
[![GoDoc](https://godoc.org/https://godoc.org/github.com/krasun/lsmtree?status.svg)](https://godoc.org/github.com/krasun/lsmtree)

lsmtree is a log-structured merge-tree implementation in Go.

**Attention!** lsmtree is **not** goroutine-safe - calling any methods on it from a different goroutine without synchronization is not safe and might lead to data corruption.  Make sure that the access is synchronized if the tree is used from the separate goroutines.

## Installation

As simple as:

```
go get github.com/krasun/lsmtree
```

## Quickstart

A fully-featured example:

```go
package lsmtree_test

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/krasun/lsmtree"
)

func Example() {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dbDir, err))
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	tree, err := lsmtree.Open(dbDir, lsmtree.SparseKeyDistance(64), lsmtree.MemTableThreshold(1000000))
	if err != nil {
		panic(fmt.Errorf("failed to open LSM tree %s: %w", dbDir, err))
	}

	err = tree.Put([]byte("Hi!"), []byte("Hello world, LSMTree!"))
	if err != nil {
		panic(fmt.Errorf("failed to put: %w", err))
	}

	err = tree.Put([]byte("Does it override key?"), []byte("No!"))
	if err != nil {
		panic(fmt.Errorf("failed to put: %w", err))
	}

	err = tree.Put([]byte("Does it override key?"), []byte("Yes, absolutely! The key has been overridden."))
	if err != nil {
		panic(fmt.Errorf("failed to put: %w", err))
	}

	if err := tree.Close(); err != nil {
		panic(fmt.Errorf("failed to close: %w", err))
	}

	tree, err = lsmtree.Open(dbDir)
	if err != nil {
		panic(fmt.Errorf("failed to open LSM tree %s: %w", dbDir, err))
	}

	value, ok, err := tree.Get([]byte("Hi!"))
	if err != nil {
		panic(fmt.Errorf("failed to get value: %w", err))
	}
	if !ok {
		fmt.Println("failed to find value")
	}

	fmt.Println(string(value))

	value, ok, err = tree.Get([]byte("Does it override key?"))
	if err != nil {
		panic(fmt.Errorf("failed to get value: %w", err))
	}
	if !ok {
		fmt.Println("failed to find value")
	}

	if err := tree.Close(); err != nil {
		panic(fmt.Errorf("failed to close: %w", err))
	}

	fmt.Println(string(value))
	// Output:
	// Hello world, LSMTree!
	// Yes, absolutely! The key has been overridden.
}
```

## Tests 

To make sure that the code is fully tested and covered:

```
$ go test .
ok  	github.com/krasun/lsmtree	2.888s
```

## Known Usages 

1. [krasun/gosqldb](https://github.com/krasun/gosqldb) - my experimental implementation of a simple database.

## License 

lsmtree is released under [the MIT license](LICENSE).