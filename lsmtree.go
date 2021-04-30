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

// LSMTree (https://en.wikipedia.org/wiki/Log-structured_merge-tree)
// is goroutine-safe log-structure merge-tree implementation for storing data in files.
type LSMTree struct {
	// Before executing any write operation,
	// it is written to the write-ahead log (WAL) and only then applied.
	wal *os.File
	// All changes that are flushed to the WAL, but not flushed
	// to the sorted files, are stored in memory for faster lookups.
	memTable *rbytree.Tree
	// The path to the directory that stores LSM tree files,
	// it is required to provide dedicated directory for each
	// instance of the tree.
	dbDir string
	// Global read-write lock for the tree. Only writer is allowed at time.
	rwlock *sync.RWMutex
}

// Open opens the database. Only one instance of the tree is allowed to
// read and write to the directory.
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

// Close closes all allocated resources.
func (s *LSMTree) Close() error {
	if err := s.wal.Close(); err != nil {
		return fmt.Errorf("failed to close file %s: %w", s.wal.Name(), err)
	}

	return nil
}

// Put puts the key into the db.
func (s *LSMTree) Put(key []byte, value []byte) error {
	if key == nil || value == nil {
		return fmt.Errorf("key/value can not be nil")
	}

	s.rwlock.Lock()
	defer s.rwlock.Unlock()

	if err := putEntry(s.wal, key, value); err != nil {
		return fmt.Errorf("failed to append to file %s: %w", s.wal.Name(), err)
	}

	s.memTable.Put(key, value)

	return nil
}

// Get the value for the key from the db.
func (s *LSMTree) Get(key []byte) ([]byte, bool, error) {
	s.rwlock.RLock()
	defer s.rwlock.RLock()

	value, _ := s.memTable.Get(key)
	if value == nil {
		// special case for deleted entry
		return nil, false, nil
	}

	return value, true, nil
}

// Delete delete the value by key from the db.
func (s *LSMTree) Delete(key []byte) error {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()

	if err := deleteEntry(s.wal, key); err != nil {
		return fmt.Errorf("failed to append to file %s: %w", s.wal.Name(), err)
	}

	// special case for deleted entry, since it is also must be flushed
	// to the file system
	s.memTable.Put(key, nil)

	return nil
}
