package lsmtree

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"testing"
)

func TestMergeDiskTables(t *testing.T) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}()
	if err := createDiskTable(prepareMemTable1(), dbDir, 0, 3); err != nil {
		t.Fatal(err)
	}
	if err := createDiskTable(prepareMemTable2(), dbDir, 1, 3); err != nil {
		t.Fatal(err)
	}

	if err := mergeDiskTables(dbDir, 0, 1, 3); err != nil {
		t.Fatal(err)
	}

	it, err := newDataFileIterator(path.Join(dbDir, "1-data.db"))
	if err != nil {
		t.Fatal(err)
	}

	actual := make([][]byte, 0)
	for it.hasNext() {
		key, value, err := it.next()
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		actual = append(actual, key, value)
	}

	expected := [][]byte{
		[]byte("b"), []byte("vb2"),
		[]byte("c"), []byte("vc"),
		[]byte("d"), nil,
		[]byte("e"), []byte("ve"),
		[]byte("f"), []byte("vf2"),
		[]byte("g"), []byte("vg"),
		[]byte("h"), []byte("vh"),
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("%s != %s", expected, actual)
	}
}

func prepareMemTable1() *memTable {
	memTable := newMemTable()

	memTable.put([]byte("b"), []byte("vb1"))
	memTable.put([]byte("c"), []byte("vc"))
	memTable.put([]byte("d"), []byte("vd"))
	memTable.put([]byte("e"), []byte("ve"))
	memTable.put([]byte("f"), []byte("vf1"))
	memTable.put([]byte("g"), []byte("vg"))
	memTable.put([]byte("h"), []byte("vh"))

	return memTable
}

func prepareMemTable2() *memTable {
	memTable := newMemTable()

	memTable.put([]byte("b"), []byte("vb2"))
	memTable.delete([]byte("d"))
	memTable.put([]byte("f"), []byte("vf2"))

	return memTable
}
