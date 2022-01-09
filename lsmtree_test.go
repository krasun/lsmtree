package lsmtree_test

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
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

	var largeValue [65536]byte
	err = tree.Put([]byte("some key"), largeValue[:])
	if !errors.Is(err, lsmtree.ErrValueTooLarge) {
		t.Fatalf("expected %v, but got %v", lsmtree.ErrValueTooLarge, err)
	}
}

func TestPut100(t *testing.T) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		panic(fmt.Errorf("failed to create %s: %w", dbDir, err))
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()

	tree, err := lsmtree.Open(
		dbDir,
		lsmtree.SparseKeyDistance(64),
		lsmtree.MemTableThreshold(100),
		lsmtree.DiskTableNumThreshold(3),
	)
	if err != nil {
		panic(fmt.Errorf("failed to open LSM tree %s: %w", dbDir, err))
	}

	fmt.Println(tree)

	// key = "1", value = "2"
	// key = "2", value = "4"
	// ...
	for i := 1; i <= 100; i++ {
		key := strconv.Itoa(i)
		value := strconv.Itoa(i * 2)
		err := tree.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	}

	// key = "1", value = "2"
	// key = "2", value deleted
	// key = "3", value = "6"
	// key = "4", value deleted
	// ...
	for i := 1; i <= 100; i++ {
		if i%2 == 0 {
			key := strconv.Itoa(i)
			err := tree.Delete([]byte(key))
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		}
	}

	tree, err = lsmtree.Open(dbDir)
	if err != nil {
		panic(fmt.Errorf("failed to open LSM tree %s: %w", dbDir, err))
	}

	for i := 1; i <= 100; i++ {
		key := strconv.Itoa(i)
		value, ok, err := tree.Get([]byte(key))
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		if i%2 == 0 && ok {
			t.Fatalf("key must be deleted %s, but it is not", key)
		}

		if i%2 != 0 {
			if !ok {
				t.Fatalf("key must be present %s, but it is not", key)
			} else {
				expectedValue := strconv.Itoa(i * 2)
				if expectedValue != string(value) {
					t.Fatalf("value is wrong for key %s: %s != %s", key, expectedValue, value)
				}
			}
		}
	}

	if err := tree.Close(); err != nil {
		panic(fmt.Errorf("failed to close: %w", err))
	}
}
