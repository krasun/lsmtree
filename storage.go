package lsmtree

import (
	"fmt"
	"os"

	"github.com/krasun/rbytree"
)

type Storage struct {
	entries *rbytree.Tree
	file    *os.File
}

func Open(path string) (*Storage, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}

	entries, err := loadEntries(file)
	if err != nil {
		return nil, fmt.Errorf("failed to load entries from %s: %w", path, err)
	}

	return &Storage{file: file, entries: entries}, nil
}

func (s *Storage) Close() error {
	if err := s.file.Close(); err != nil {
		return fmt.Errorf("failed to close file %s: %w", s.file.Name(), err)
	}

	return nil
}

func (s *Storage) Put(key []byte, value []byte) error {
	if key == nil || value == nil {
		return fmt.Errorf("key/value can not be nil")
	}

	if err := putEntry(s.file, key, value); err != nil {
		return fmt.Errorf("failed to append to file %s: %w", s.file.Name(), err)
	}

	s.entries.Put(key, value)

	return nil
}

func (s *Storage) Get(key []byte) ([]byte, bool, error) {
	value, _ := s.entries.Get(key)
	if value == nil {
		// special case for deleted entry
		return nil, false, nil
	}

	return value, true, nil
}

func (s *Storage) Delete(key []byte) error {
	if err := deleteEntry(s.file, key); err != nil {
		return fmt.Errorf("failed to append to file %s: %w", s.file.Name(), err)
	}

	// special case for deleted entry
	s.entries.Put(key, nil)

	return nil
}
