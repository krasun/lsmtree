package lsmtree_test

import (
	"fmt"
	"os"
	"path"

	"github.com/krasun/lsmtree"
)

func Example() {
	dbDir := path.Join(os.TempDir(), "lsmtree_example")
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	tree, err := lsmtree.Open(dbDir)
	if err != nil {
		panic(fmt.Errorf("failed to open LSM tree %s: %w", dbDir, err))
	}

	err = tree.Put([]byte("Hi!"), []byte("Hello world, LSMTree!"))
	if err != nil {
		panic(fmt.Errorf("failed to put: %w", err))
	}

	value, ok, err := tree.Get([]byte("Hi!"))
	if err != nil {
		panic(fmt.Errorf("failed to get value: %w", err))
	}
	if !ok {
		fmt.Println("failed to find value")
	}

	fmt.Println(string(value))

	err = tree.Put([]byte("Does it override key?"), []byte("No!"))
	if err != nil {
		panic(fmt.Errorf("failed to put: %w", err))
	}

	err = tree.Put([]byte("Does it override key?"), []byte("Yes, absolutely! The key has been overridden."))
	if err != nil {
		panic(fmt.Errorf("failed to put: %w", err))
	}

	value, ok, err = tree.Get([]byte("Does it override key?"))
	if err != nil {
		panic(fmt.Errorf("failed to get value: %w", err))
	}
	if !ok {
		fmt.Println("failed to find value")
	}

	fmt.Println(string(value))
	// Output: 
	// Hello world, LSMTree!
	// Yes, absolutely! The key has been overridden.
}
