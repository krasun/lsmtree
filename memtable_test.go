package lsmtree

import (
	"math/rand"
	"testing"
	"time"
)

func TestMemTable_put(t *testing.T) {
	const keySize = 64
	const valueSize = 1024
	const length = 100
	mt := newMemTable()
	for i := 0; i < length; i++ {
		err := mt.put(randBytes(keySize), randBytes(valueSize))
		if err != nil {
			t.Error(err)
		}
	}
	if mt.data.Size() != length {
		t.Errorf("length of underlying tree is not as expected, expected: %d, actual: %d", length, mt.data.Size())
	}
	expectedSize := (keySize + valueSize) * length
	if mt.bytes() != expectedSize {
		t.Errorf("size of data is not as expected, expected: %d, actual: %d", expectedSize, mt.bytes())
	}
}

func TestMemTable_get(t *testing.T) {
	const length = 100
	mt := newMemTable()
	keys := make([][]byte, 0, length)
	for i := 0; i < length; i++ {
		key := randBytes(64)
		keys = append(keys, key)
		err := mt.put(key, randBytes(1024))
		if err != nil {
			t.Error(err)
		}
	}
	for _, k := range keys {
		_, ok := mt.get(k)
		if !ok {
			t.Error("the key does not exist in memtable")
		}
	}
}

func TestMemTable_delete(t *testing.T) {
	const keySize = 64
	const length = 100
	mt := newMemTable()
	keys := make([][]byte, 0, length)
	for i := 0; i < length; i++ {
		key := randBytes(keySize)
		keys = append(keys, key)
		err := mt.put(key, randBytes(1024))
		if err != nil {
			t.Error(err)
		}
	}
	for _, k := range keys {
		err := mt.delete(k)
		if err != nil {
			t.Error(err)
		}
	}
	if mt.data.Size() != length {
		t.Errorf("length of underlying tree is not as expected, expected: %d, actual: %d", length, mt.data.Size())
	}
	expectedSize := keySize * length
	if mt.bytes() != expectedSize {
		t.Errorf("size of data is not as expected, expected: %d, actual: %d", expectedSize, mt.bytes())
	}
}

func TestMemTable_clear(t *testing.T) {
	const length = 100
	mt := newMemTable()
	for i := 0; i < length; i++ {
		err := mt.put(randBytes(64), randBytes(1024))
		if err != nil {
			t.Error(err)
		}
	}
	mt.clear()
	if mt.data.Size() != 0 {
		t.Errorf("length of underlying tree is not zero, actual: %d", mt.data.Size())
	}
	if mt.bytes() != 0 {
		t.Errorf("size of data is not zero, actual: %d", mt.bytes())
	}
}

var r = rand.New(rand.NewSource(time.Now().Unix()))

func randBytes(size int) []byte {
	b := make([]byte, 0, size)
	for i := 0; i < size; i++ {
		b = append(b, byte(r.Uint32()))
	}
	return b
}
