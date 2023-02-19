package lsmtree

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path"
)

const (
	// MaxKeySize is the maximum allowed key size.
	// The size is hard-coded and must not be changed since it has
	// impact on the encoding features.
	MaxKeySize = math.MaxUint16
	// MaxValueSize is the maximum allowed value size.
	// The size is hard-coded and must not be changed since it has
	// impact on the encoding features.
	MaxValueSize = math.MaxUint16
)

const (
	// WAL file name.
	walFileName = "wal.db"
	// Default MemTable table threshold.
	defaultMemTableThreshold = 64000 // 64 kB
	// Default distance between keys in sparse index.
	defaultSparseKeyDistance = 128
	// Default DiskTable number threshold.
	defaultDiskTableNumThreshold = 10
)

var (
	// ErrKeyRequired is returned when putting a zero-length key or nil.
	ErrKeyRequired = errors.New("key required")
	// ErrValueRequired is returned when putting a zero-length value or nil.
	ErrValueRequired = errors.New("value required")
	// ErrKeyTooLarge is returned when putting a key that is larger than MaxKeySize.
	ErrKeyTooLarge = errors.New("key too large")
	// ErrValueTooLarge is returned when putting a value that is larger than MaxValueSize.
	ErrValueTooLarge = errors.New("value too large")
)

// LSMTree (https://en.wikipedia.org/wiki/Log-structured_merge-tree)
// is log-structure merge-tree implementation for storing data in files.
// The implementation is not goroutine-safe! Make sure that if needed the access
// to the tree is synchronized.
type LSMTree struct {
	// The path to the directory that stores LSM tree files,
	// it is required to provide dedicated directory for each
	// instance of the tree.
	dbDir string

	// Before executing any write operation,
	// it is written to the write-ahead log (WAL) and only then applied.
	wal *os.File

	// It points to the latest created DiskTable on the disk. After
	// MemTable is flushed, the index is updated.
	// By default -1 to denote that there is no DiskTable.
	maxDiskTableIndex int

	// Current number of flushed and merged disk tables in the durable storage.
	diskTableNum int

	// All changes that are flushed to the WAL, but not flushed
	// to the sorted files, are stored in memory for faster lookups.
	memTable *memTable

	// If MemTable size in bytes passes the threshold, it must
	// be flushed to the filesystem.
	memTableThreshold int

	// If DiskTable number passes the threshold, disk tables must be
	// merged to decrease it.
	diskTableNumThreshold int

	// Distance between keys in sparse index.
	sparseKeyDistance int
}

// MemTableThreshold sets memTableThreshold for LSMTree.
// If MemTable size in bytes passes the threshold, it must
// be flushed to the filesystem.
func MemTableThreshold(memTableThreshold int) func(*LSMTree) {
	return func(t *LSMTree) {
		t.memTableThreshold = memTableThreshold
	}
}

// SparseKeyDistance sets sparseKeyDistance for LSMTree.
// Distance between keys in sparse index.
func SparseKeyDistance(sparseKeyDistance int) func(*LSMTree) {
	return func(t *LSMTree) {
		t.sparseKeyDistance = sparseKeyDistance
	}
}

// DiskTableNumThreshold sets diskTableNumThreshold for LSMTree.
// If DiskTable number passes the threshold, disk tables must be
// merged to decrease it.
func DiskTableNumThreshold(diskTableNumThreshold int) func(*LSMTree) {
	return func(t *LSMTree) {
		t.diskTableNumThreshold = diskTableNumThreshold
	}
}

// Open opens the database. Only one instance of the tree is allowed to
// read and write to the directory.
func Open(dbDir string, options ...func(*LSMTree)) (*LSMTree, error) {
	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("directory %s does not exist", dbDir)
	}

	walPath := path.Join(dbDir, walFileName)
	wal, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", walPath, err)
	}

	memTable, err := loadMemTable(wal)
	if err != nil {
		return nil, fmt.Errorf("failed to load entries from %s: %w", walPath, err)
	}

	diskTableNum, maxDiskTableIndex, err := readDiskTableMeta(dbDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read disk table meta: %w", err)
	}

	t := &LSMTree{
		wal:                   wal,
		memTable:              memTable,
		dbDir:                 dbDir,
		maxDiskTableIndex:     maxDiskTableIndex,
		memTableThreshold:     defaultMemTableThreshold,
		sparseKeyDistance:     defaultSparseKeyDistance,
		diskTableNum:          diskTableNum,
		diskTableNumThreshold: defaultDiskTableNumThreshold,
	}
	for _, option := range options {
		option(t)
	}

	return t, nil
}

// Close closes all allocated resources.
func (t *LSMTree) Close() error {
	if err := t.wal.Close(); err != nil {
		return fmt.Errorf("failed to close file %s: %w", t.wal.Name(), err)
	}

	return nil
}

// Put puts the key into the db.
func (t *LSMTree) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyRequired
	} else if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	} else if len(value) == 0 {
		return ErrValueRequired
	} else if uint64(len(value)) > MaxValueSize {
		return ErrValueTooLarge
	}

	if err := appendToWAL(t.wal, key, value); err != nil {
		return fmt.Errorf("failed to append to file %s: %w", t.wal.Name(), err)
	}

	t.memTable.put(key, value)

	if t.memTable.bytes() >= t.memTableThreshold {
		if err := t.flushMemTable(); err != nil {
			return fmt.Errorf("failed to flush MemTable: %w", err)
		}
	}

	if t.diskTableNum >= t.diskTableNumThreshold {
		oldest := t.maxDiskTableIndex - t.diskTableNum + 1
		if err := mergeDiskTables(t.dbDir, oldest, oldest+1, t.sparseKeyDistance); err != nil {
			return fmt.Errorf("failed to merge disk tables: %w", err)
		}

		newDiskTableNum := t.diskTableNum - 1
		if err := updateDiskTableMeta(t.dbDir, newDiskTableNum, t.maxDiskTableIndex); err != nil {
			return fmt.Errorf("failed to update disk table meta: %w", err)
		}

		t.diskTableNum--
	}

	return nil
}

// Get the value for the key from the db.
func (t *LSMTree) Get(key []byte) ([]byte, bool, error) {
	value, exists := t.memTable.get(key)
	if exists {
		return value, value != nil, nil
	}

	value, exists, err := searchInDiskTables(t.dbDir, t.maxDiskTableIndex, key)
	if err != nil {
		return nil, false, fmt.Errorf("failed to search in DiskTables: %w", err)
	}

	return value, exists, nil
}

// Delete delete the value by key from the db.
func (t *LSMTree) Delete(key []byte) error {
	if err := appendToWAL(t.wal, key, nil); err != nil {
		return fmt.Errorf("failed to append to file %s: %w", t.wal.Name(), err)
	}

	t.memTable.delete(key)

	return nil
}

// flushMemTable flushes current MemTable onto the disk and clears it.
// The function expects it to run in the synchronized block,
// and thus it does not use any synchronization mechanisms.
func (t *LSMTree) flushMemTable() error {
	newDiskTableNum := t.diskTableNum + 1
	newDiskTableIndex := t.maxDiskTableIndex + 1

	if err := createDiskTable(t.memTable, t.dbDir, newDiskTableIndex, t.sparseKeyDistance); err != nil {
		return fmt.Errorf("failed to create disk table %d: %w", newDiskTableIndex, err)
	}

	if err := updateDiskTableMeta(t.dbDir, newDiskTableNum, newDiskTableIndex); err != nil {
		return fmt.Errorf("failed to update max disk table index %d: %w", newDiskTableIndex, err)
	}

	newWAL, err := clearWAL(t.dbDir, t.wal)
	if err != nil {
		return fmt.Errorf("failed to clear the WAL file: %w", err)
	}

	t.wal = newWAL
	t.memTable.clear()
	t.diskTableNum = newDiskTableNum
	t.maxDiskTableIndex = newDiskTableIndex

	return nil
}
