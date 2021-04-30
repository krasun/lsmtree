package lsmtree

import (
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/krasun/rbytree"
)

// WAL file name
const walFileName = "wal"

// LSMTree is goroutine-safe log-structure merge-tree implementation for storing
// data in files.
//
// https://en.wikipedia.org/wiki/Log-structured_merge-tree
type LSMTree struct {
	// before executing any write operation,
	// it is written to the write-ahead log (WAL) and only then applied
	wal *os.File
	// all changes that are flushed to the WAL, but not flushed
	// to the sorted files, are stored in memory for faster lookups
	memTable *rbytree.Tree
	// dbDir to the directory that stores LSM tree files,
	// it is required to provide dedicated directory for each
	// instance of the tree
	dbDir string
	// global read-write lock for the tree
	rwlock *sync.RWMutex
}

func Open(dbDir string) (*LSMTree, error) {
	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		err := os.Mkdir(dbDir, 0600)
		if err != nil {
			return nil, fmt.Errorf("failed to mkdir %s: %w", dbDir, err)
		}
	}

	walPath := path.Join(dbDir, walFileName)
	f, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", walPath, err)
	}

	memTable, err := loadMemTable(f)
	if err != nil {
		return nil, fmt.Errorf("failed to load entries from %s: %w", walPath, err)
	}

	return &LSMTree{wal: f, memTable: memTable, dbDir: dbDir, rwlock: &sync.RWMutex{}}, nil
}

func (s *LSMTree) Close() error {
	if err := s.wal.Close(); err != nil {
		return fmt.Errorf("failed to close file %s: %w", s.wal.Name(), err)
	}

	return nil
}

func (s *LSMTree) Put(key []byte, value []byte) error {
	if key == nil || value == nil {
		return fmt.Errorf("key/value can not be nil")
	}

	if err := putEntry(s.wal, key, value); err != nil {
		return fmt.Errorf("failed to append to file %s: %w", s.wal.Name(), err)
	}

	s.memTable.Put(key, value)

	return nil
}

func (s *LSMTree) Get(key []byte) ([]byte, bool, error) {
	value, _ := s.memTable.Get(key)
	if value == nil {
		// special case for deleted entry
		return nil, false, nil
	}

	return value, true, nil
}

func (s *LSMTree) Delete(key []byte) error {
	if err := deleteEntry(s.wal, key); err != nil {
		return fmt.Errorf("failed to append to file %s: %w", s.wal.Name(), err)
	}

	// special case for deleted entry, since it is also must be flushed
	// to the file system
	s.memTable.Put(key, nil)

	return nil
}
