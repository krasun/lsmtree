package lsmtree

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"testing"
)

func TestSearchInDiskTables(t *testing.T) {
	dbDir, close, err := prepareDiskTable(prepareMemTable(), 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	cases := []struct {
		maxIndex int
		key      []byte
		value    []byte
		ok       bool
		hasErr   bool
	}{
		{0, []byte("a"), nil, false, false},
		{0, []byte("b"), []byte("vb"), true, false},
		{0, []byte("c"), []byte("vc"), true, false},
		{0, []byte("f"), []byte("vf"), true, false},
		{0, []byte("f"), []byte("vf"), true, false},
		{0, []byte("k"), nil, false, false},
		{1, []byte("b"), nil, false, true},
	}

	for _, c := range cases {
		value, ok, err := searchInDiskTables(dbDir, c.maxIndex, c.key)
		if c.hasErr && err == nil {
			t.Fatalf("err == nil, but must be returned for %s: %v != %v", string(c.key), c.ok, ok)
		}

		if !c.hasErr {
			if !((c.value == nil && value == nil) || (bytes.Equal(c.value, value))) {
				t.Fatalf("values do not match for %s, err = %v: %s != %s", string(c.key), err, string(c.value), string(value))
			}
			if c.ok != ok {
				t.Fatalf("ok does not match for %s, err = %v, value = %s: %v != %v", string(c.key), err, string(value), c.ok, ok)
			}
		}
	}
}

func TestSearchInDiskTable(t *testing.T) {
	dbDir, close, err := prepareDiskTable(prepareMemTable(), 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	cases := []struct {
		index  int
		key    []byte
		value  []byte
		ok     bool
		hasErr bool
	}{
		{0, []byte("a"), nil, false, false},
		{0, []byte("b"), []byte("vb"), true, false},
		{0, []byte("c"), []byte("vc"), true, false},
		{0, []byte("f"), []byte("vf"), true, false},
		{0, []byte("f"), []byte("vf"), true, false},
		{0, []byte("k"), nil, false, false},
		{1, []byte("b"), nil, false, true},
	}

	for _, c := range cases {
		value, ok, err := searchInDiskTable(dbDir, c.index, c.key)
		if c.hasErr && err == nil {
			t.Fatalf("err == nil, but must be returned for %s: %v != %v", string(c.key), c.ok, ok)
		}

		if !c.hasErr {
			if !((c.value == nil && value == nil) || (bytes.Equal(c.value, value))) {
				t.Fatalf("values do not match for %s, err = %v: %s != %s", string(c.key), err, string(c.value), string(value))
			}
			if c.ok != ok {
				t.Fatalf("ok does not match for %s, err = %v, value = %s: %v != %v", string(c.key), err, string(value), c.ok, ok)
			}
		}
	}
}

func TestSearchInDataFile(t *testing.T) {
	dbDir, close, err := prepareDiskTable(prepareMemTable(), 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	cases := []struct {
		key    []byte
		value  []byte
		ok     bool
		hasErr bool
		offset int
	}{
		{[]byte("a"), nil, false, false, 0},
		{[]byte("b"), []byte("vb"), true, false, 0},
		{[]byte("b"), nil, false, false, 19},
		{[]byte("c"), []byte("vc"), true, false, 19},
		{[]byte("f"), []byte("vf"), true, false, 0},
		{[]byte("f"), []byte("vf"), true, false, 76},
		{[]byte("k"), nil, false, false, 0},
	}

	for _, c := range cases {
		value, ok, err := searchInDataFile(path.Join(dbDir, "0-data.db"), c.offset, c.key)
		if !((c.value == nil && value == nil) || (bytes.Equal(c.value, value))) {
			t.Fatalf("values do not match for %s, err = %v: %s != %s", string(c.key), err, string(c.value), string(value))
		}
		if c.ok != ok {
			t.Fatalf("ok does not match for %s, err = %v, value = %s: %v != %v", string(c.key), err, string(value), c.ok, ok)
		}
		if c.hasErr && err == nil {
			t.Fatalf("err == nil, but must be returned for %s: %v != %v", string(c.key), c.ok, ok)
		}
	}
}

func TestSearchInIndex(t *testing.T) {
	dbDir, close, err := prepareDiskTable(prepareMemTable(), 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	cases := []struct {
		key      []byte
		from, to int
		ok       bool
		hasErr   bool
		offset   int
	}{
		{[]byte("a"), 0, 1000, false, false, 0},
		{[]byte("b"), 0, 0, true, false, 0},
		{[]byte("c"), 0, 75, true, false, 19},
		{[]byte("f"), 75, 150, true, false, 76},
		{[]byte("k"), 150, 0, false, false, 0},
	}

	for _, c := range cases {
		offset, ok, err := searchInIndex(path.Join(dbDir, "0-index.db"), c.from, c.to, c.key)
		if c.offset != offset {
			t.Fatalf("offset does not match for %s, err = %v: %d != %d", string(c.key), err, c.offset, offset)
		}
		if c.ok != ok {
			t.Fatalf("ok does not match for %s, err = %v, offset = %d: %v != %v", string(c.key), err, offset, c.ok, ok)
		}
		if c.hasErr && err == nil {
			t.Fatalf("err == nil, but must be returned for %s: %v != %v", string(c.key), c.ok, ok)
		}
	}
}

func TestSearchInSparseIndex(t *testing.T) {
	dbDir, close, err := prepareDiskTable(prepareMemTable(), 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	cases := []struct {
		key      []byte
		from, to int
		ok       bool
		hasErr   bool
	}{
		{[]byte("a"), 0, 0, false, false},
		{[]byte("b"), 0, 0, true, false},
		{[]byte("c"), 0, 75, true, false},
		{[]byte("f"), 75, 150, true, false},
		{[]byte("k"), 150, 0, true, false},
	}

	for _, c := range cases {
		from, to, ok, err := searchInSparseIndex(path.Join(dbDir, "0-sparse.db"), c.key)
		if c.from != from || c.to != to {
			t.Fatalf("from and to do not match for %s, err = %v: %d != %d or %d != %d", string(c.key), err, c.from, from, c.to, to)
		}
		if c.ok != ok {
			t.Fatalf("ok does not match for %s, err = %v, from = %d, to = %d: %v != %v", string(c.key), err, from, to, c.ok, ok)
		}
		if c.hasErr && err == nil {
			t.Fatalf("err == nil, but must be returned for %s: %v != %v", string(c.key), c.ok, ok)
		}
	}
}

func TestDataFileIterator(t *testing.T) {
	dbDir, close, err := prepareDiskTable(prepareMemTable(), 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	it, err := newDataFileIterator(path.Join(dbDir, "0-data.db"))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
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
		[]byte("b"), []byte("vb"),
		[]byte("c"), []byte("vc"),
		[]byte("d"), []byte("vd"),
		[]byte("e"), []byte("ve"),
		[]byte("f"), []byte("vf"),
		[]byte("g"), []byte("vg"),
		[]byte("h"), []byte("vh"),
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("%v != %v", expected, actual)
	}
}

func prepareDiskTable(memTable *memTable, index, sparseKeyDistance int) (string, func(), error) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "example")
	if err != nil {
		return "", nil, err
	}

	err = createDiskTable(memTable, dbDir, index, sparseKeyDistance)
	if err != nil {
		return "", nil, err
	}

	return dbDir, func() {
		if err := os.RemoveAll(dbDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", dbDir, err))
		}
	}, nil
}

func prepareMemTable() *memTable {
	memTable := newMemTable()

	memTable.put([]byte("b"), []byte("vb"))
	memTable.put([]byte("c"), []byte("vc"))
	memTable.put([]byte("d"), []byte("vd"))
	memTable.put([]byte("e"), []byte("ve"))
	memTable.put([]byte("f"), []byte("vf"))
	memTable.put([]byte("g"), []byte("vg"))
	memTable.put([]byte("h"), []byte("vh"))

	return memTable
}
