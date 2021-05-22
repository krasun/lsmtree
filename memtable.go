package lsmtree

import (
	"github.com/krasun/rbytree"
)

// MemTable. All changes that are flushed to the WAL, but not flushed
// to the sorted files, are stored in memory for faster lookups.
// A red-black instance might be used directly, but the wrapper and additional
// layer of abstraction simplifies further changes.
type memTable struct {
	data *rbytree.Tree
	// The size of all keys and values inserted into the MemTable in b.
	b int
}

// newMemTable returns a new instance of the MemTable.
func newMemTable() *memTable {
	return &memTable{data: rbytree.New(), b: 0}
}

// put puts the key and the value into the table.
func (mt *memTable) put(key, value []byte) error {
	prev, exists := mt.data.Put(key, value)
	if exists {
		mt.b += -len(prev) + len(value)
	} else {
		mt.b += len(key) + len(value)
	}

	return nil
}

// get returns the value by the key.
// Caution! Get returns true for the removed keys in the memory.
func (mt *memTable) get(key []byte) ([]byte, bool) {
	return mt.data.Get(key)
}

// delete marks the key as deleted in the table, but does not remove it.
func (mt *memTable) delete(key []byte) error {
	value, exists := mt.data.Put(key, nil)
	if !exists {
		mt.b -= len(value)
	}

	return nil
}

// bytes returns the size of all keys and values inserted into the MemTable in bytes.
func (mt *memTable) bytes() int {
	return mt.b
}

// clear clears all the data and resets the size.
func (mt *memTable) clear() {
	mt.data = rbytree.New()
	mt.b = 0
}

// iterator returns iterator for the MemTable. It also iterates over
// deleted keys, but the value for them is nil.
func (mt *memTable) iterator() *memTableIterator {
	return &memTableIterator{mt.data.Iterator()}
}

// MemTable iterator.
type memTableIterator struct {
	it *rbytree.Iterator
}

// hasNext returns true if there is next element.
func (it *memTableIterator) hasNext() bool {
	return it.it.HasNext()
}

// next returns the current key and value and advances the iterator position.
func (it *memTableIterator) next() ([]byte, []byte) {
	return it.it.Next()
}
