package lsmtree

import (
	"fmt"
	"io"
	"os"

	"github.com/krasun/rbytree"
)

func putEntry(file *os.File, key []byte, value []byte) error {
	return appendToFile(file, encodePut(key, value))
}

func deleteEntry(file *os.File, key []byte) error {
	return appendToFile(file, encodeDelete(key))
}

func appendToFile(file *os.File, data []byte) error {
	// for safety, since the file is open in read-write mode
	if _, err := file.Seek(0, 2); err != nil {
		return fmt.Errorf("failed to seek to the end: %w", err)
	}

	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("failed to write to the file: %w", err)
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync the file: %w", err)
	}

	return nil
}

func loadMemTable(file *os.File) (*rbytree.Tree, error) {
	memTable := rbytree.New()
	deleteKeys := make([][]byte, 0)
	offset := 0
	for {
		var encodedEntryLen [8]byte
		n, err := file.ReadAt(encodedEntryLen[:], int64(offset))
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read: %w", err)
		}
		if n < 8 && err == io.EOF {
			for _, deleteKey := range deleteKeys {
				// we need to mark deleted key, to make sure
				// it will be written to the sorted file
				memTable.Put(deleteKey, nil)
			}

			return memTable, nil
		}
		offset += n

		entryLen := decodeLen(encodedEntryLen[:])
		encodedEntry := make([]byte, entryLen)
		n, err = file.ReadAt(encodedEntry, int64(offset))

		if n < entryLen {
			return nil, fmt.Errorf("the file is corrupted, failed to read entry: %w", err)
		}
		offset += n

		key, value, deleted := decode(encodedEntry)
		if deleted {
			deleteKeys = append(deleteKeys, key)
		} else {
			memTable.Put(key, value)
		}
	}
}
