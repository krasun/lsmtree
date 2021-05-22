package lsmtree_test

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

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

	tree, err := lsmtree.Open(dbDir)
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

func TestPutForErrors(t *testing.T) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dbDir, err))
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	tree, err := lsmtree.Open(dbDir)
	if err != nil {
		panic(fmt.Errorf("failed to open LSM tree %s: %w", dbDir, err))
	}

	err = tree.Put(nil, []byte("some value"))
	if !errors.Is(err, lsmtree.ErrKeyRequired) {
		t.Fatalf("expected %v, but got %v", lsmtree.ErrKeyRequired, err)
	}

	err = tree.Put([]byte{}, []byte("some value"))
	if !errors.Is(err, lsmtree.ErrKeyRequired) {
		t.Fatalf("expected %v, but got %v", lsmtree.ErrKeyRequired, err)
	}

	err = tree.Put([]byte("some key"), nil)
	if !errors.Is(err, lsmtree.ErrValueRequired) {
		t.Fatalf("expected %v, but got %v", lsmtree.ErrValueRequired, err)
	}

	err = tree.Put([]byte("some key"), []byte{})
	if !errors.Is(err, lsmtree.ErrValueRequired) {
		t.Fatalf("expected %v, but got %v", lsmtree.ErrValueRequired, err)
	}

	var largeKey [65536]byte
	err = tree.Put(largeKey[:], []byte("some value"))
	if !errors.Is(err, lsmtree.ErrKeyTooLarge) {
		t.Fatalf("expected %v, but got %v", lsmtree.ErrKeyTooLarge, err)
	}

	var largeValue [4294967296]byte
	err = tree.Put([]byte("some key"), largeValue[:])
	if !errors.Is(err, lsmtree.ErrValueTooLarge) {
		t.Fatalf("expected %v, but got %v", lsmtree.ErrValueTooLarge, err)
	}
}
