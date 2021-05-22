package lsmtree

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
)

const (
	// DiskTable file name that contains the max disk table number.
	diskTableNumFileName = "maxdisktable"
	// DiskTable data file name. It contains raw data.
	diskTableDataFileName = "data.db"
	// DiskTable key file name. It contains keys and positions to values in the data file.
	diskTableIndexFileName = "index.db"
	// DiskTable sparse index. A sampling of every 64th entry in the index file.
	diskTableSparseIndexFileName = "sparse.db"
	// A flag to open file for new disk table files: data, index and sparse index.
	newDiskTableFlag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC | os.O_APPEND
)

// createDiskTable creates a DiskTable from the given MemTable with the given prefix
// and in the given directory.
func createDiskTable(memTable *memTable, dbDir string, index, sparseKeyDistance int) error {
	prefix := strconv.Itoa(index) + "-"

	w, err := newDiskTableWriter(dbDir, prefix, sparseKeyDistance)
	if err != nil {
		return fmt.Errorf("failed to create disk table writer: %w", err)
	}

	for it := memTable.iterator(); it.hasNext(); {
		key, value := it.next()
		if err := w.write(key, value); err != nil {
			return fmt.Errorf("failed to write to disk table %d: %w", index, err)
		}
	}

	if err := w.sync(); err != nil {
		return fmt.Errorf("failed to sync disk table: %w", err)
	}

	if err := w.close(); err != nil {
		return fmt.Errorf("failed to close disk table: %w", err)
	}

	return nil
}

// searchInDiskTables searches a value by the key in DiskTables, by traversing
// all tables in the directory.
func searchInDiskTables(dbDir string, maxIndex int, key []byte) ([]byte, bool, error) {
	for index := maxIndex; index >= 0; index-- {
		value, exists, err := searchInDiskTable(dbDir, index, key)
		if err != nil {
			return nil, false, fmt.Errorf("failed to search in disk table with index %d: %w", index, err)
		}

		if exists {
			return value, exists, nil
		}
	}

	return nil, false, nil
}

// searchInDiskTable searches a given key in a given disk table.
func searchInDiskTable(dbDir string, index int, key []byte) ([]byte, bool, error) {
	prefix := strconv.Itoa(index) + "-"

	sparseIndexPath := path.Join(dbDir, prefix+diskTableSparseIndexFileName)
	sparseIndexFile, err := os.OpenFile(sparseIndexPath, os.O_RDONLY, 0600)
	if err != nil {
		return nil, false, fmt.Errorf("failed to open sparse index file: %w", err)
	}
	defer sparseIndexFile.Close()

	from, to, ok, err := searchInSparseIndex(sparseIndexFile, key)
	if err != nil {
		return nil, false, fmt.Errorf("failed to search in sparse index file %s: %w", sparseIndexPath, err)
	}
	if !ok {
		return nil, false, nil
	}

	indexPath := path.Join(dbDir, prefix+diskTableIndexFileName)
	indexFile, err := os.OpenFile(indexPath, os.O_RDONLY, 0600)
	if err != nil {
		return nil, false, fmt.Errorf("failed to open index file: %w", err)
	}
	defer indexFile.Close()

	offset, ok, err := searchInIndex(indexFile, from, to, key)
	if err != nil {
		return nil, false, fmt.Errorf("failed to search in index file %s: %w", indexPath, err)
	}
	if !ok {
		return nil, false, nil
	}

	dataPath := path.Join(dbDir, prefix+diskTableDataFileName)
	dataFile, err := os.OpenFile(dataPath, os.O_RDONLY, 0600)
	if err != nil {
		return nil, false, fmt.Errorf("failed to open data file: %w", err)
	}
	defer dataFile.Close()

	value, ok, err := searchInDataFile(dataFile, offset, key)
	if err != nil {
		return nil, false, fmt.Errorf("failed to search in data file %s: %w", dataPath, err)
	}

	if err := sparseIndexFile.Close(); err != nil {
		return nil, false, fmt.Errorf("failed to close sparse index file: %w", err)
	}

	if err := indexFile.Close(); err != nil {
		return nil, false, fmt.Errorf("failed to close index file: %w", err)
	}

	if err := dataFile.Close(); err != nil {
		return nil, false, fmt.Errorf("failed to close data file: %w", err)
	}

	return value, ok, nil
}

// searchInDataFile searches a value by the key in the data file from the given offset.
// The offset must always point to the beginning of the record.
func searchInDataFile(r io.ReadSeeker, offset int, searchKey []byte) ([]byte, bool, error) {
	if _, err := r.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, false, fmt.Errorf("failed to seek: %w", err)
	}

	for {
		key, value, err := decode(r)
		if err != nil && err != io.EOF {
			return nil, false, fmt.Errorf("failed to read: %w", err)
		}
		if err == io.EOF {
			return nil, false, nil
		}

		if bytes.Equal(key, searchKey) {
			return value, true, nil
		}
	}
}

// searchInIndex searches key in the index file in specified range.
func searchInIndex(r io.ReadSeeker, from, to int, searchKey []byte) (int, bool, error) {
	if _, err := r.Seek(int64(from), io.SeekStart); err != nil {
		return 0, false, fmt.Errorf("failed to seek: %w", err)
	}

	for {
		key, value, err := decode(r)
		if err != nil && err != io.EOF {
			return 0, false, fmt.Errorf("failed to read: %w", err)
		}
		if err == io.EOF {
			return 0, false, nil
		}
		offset := decodeInt(value)

		if bytes.Equal(key, searchKey) {
			return offset, true, nil
		}

		if to > from {
			current, err := r.Seek(0, io.SeekCurrent)
			if err != nil {
				return 0, false, fmt.Errorf("failed to seek: %w", err)
			}

			if current > int64(to) {
				return 0, false, nil
			}
		}
	}
}

// searchInSparseIndex searches a range between which the key is located.
func searchInSparseIndex(r io.Reader, searchKey []byte) (int, int, bool, error) {
	from := -1
	for {
		key, value, err := decode(r)
		if err != nil && err != io.EOF {
			return 0, 0, false, fmt.Errorf("failed to read: %w", err)
		}
		if err == io.EOF {
			return from, 0, from != -1, nil
		}
		offset := decodeInt(value)

		cmp := bytes.Compare(key, searchKey)
		if cmp == 0 {
			return offset, offset, true, nil
		} else if cmp < 0 {
			from = offset
		} else if cmp > 0 {
			if from == -1 {
				// if the first key in the sparse index is larger than
				// the search key, it means there is no key
				return 0, 0, false, nil
			} else {
				return from, offset, true, nil
			}
		}
	}
}

// renameDiskTable renames disk table: data, index and sparse index files.
func renameDiskTable(dbDir string, oldPrefix, newPrefix string) error {
	if err := os.Rename(path.Join(dbDir, oldPrefix+diskTableDataFileName), path.Join(dbDir, newPrefix+diskTableDataFileName)); err != nil {
		return fmt.Errorf("failed to rename data file: %w", err)
	}

	if err := os.Rename(path.Join(dbDir, oldPrefix+diskTableIndexFileName), path.Join(dbDir, newPrefix+diskTableIndexFileName)); err != nil {
		return fmt.Errorf("failed to rename index file: %w", err)
	}

	if err := os.Rename(path.Join(dbDir, oldPrefix+diskTableSparseIndexFileName), path.Join(dbDir, newPrefix+diskTableSparseIndexFileName)); err != nil {
		return fmt.Errorf("failed to rename sparse index file: %w", err)
	}

	return nil
}

// deleteDiskTable deletes disk table: data, index and sparse index files.
func deleteDiskTables(dbDir string, prefixes ...string) error {
	for _, prefix := range prefixes {
		dataPath := path.Join(dbDir, prefix+diskTableDataFileName)
		if err := os.Remove(dataPath); err != nil {
			return fmt.Errorf("failed to remove data file %s: %w", dataPath, err)
		}

		indexPath := path.Join(dbDir, prefix+diskTableIndexFileName)
		if err := os.Remove(indexPath); err != nil {
			return fmt.Errorf("failed to remove data file %s: %w", indexPath, err)
		}

		sparseIndexPath := path.Join(dbDir, prefix+diskTableSparseIndexFileName)
		if err := os.Remove(sparseIndexPath); err != nil {
			return fmt.Errorf("failed to remove data file %s: %w", sparseIndexPath, err)
		}
	}

	return nil
}

// diskTableWriter is a simple abstraction over the disk table, but only
// for the writing purposes.
type diskTableWriter struct {
	dataFile        *os.File
	indexFile       *os.File
	sparseIndexFile *os.File

	sparseKeyDistance int

	keyNum, dataPos, indexPos int
}

// newDiskTableWriter returns new instance of diskTableWriter.
func newDiskTableWriter(dbDir, prefix string, sparseKeyDistance int) (*diskTableWriter, error) {
	dataPath := path.Join(dbDir, prefix+diskTableDataFileName)
	dataFile, err := os.OpenFile(dataPath, newDiskTableFlag, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file %s: %w", dataPath, err)
	}

	indexPath := path.Join(dbDir, prefix+diskTableIndexFileName)
	indexFile, err := os.OpenFile(indexPath, newDiskTableFlag, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file %s: %w", indexPath, err)
	}

	sparseIndexPath := path.Join(dbDir, prefix+diskTableSparseIndexFileName)
	sparseIndexFile, err := os.OpenFile(sparseIndexPath, newDiskTableFlag, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open sparse index file %s: %w", sparseIndexPath, err)
	}

	return &diskTableWriter{
		dataFile:          dataFile,
		indexFile:         indexFile,
		sparseIndexFile:   sparseIndexFile,
		sparseKeyDistance: sparseKeyDistance,
		keyNum:            0,
		dataPos:           0,
		indexPos:          0,
	}, nil
}

// write writes key and value into the disk table: data, index and
// sparse index file.
func (w *diskTableWriter) write(key, value []byte) error {
	dataBytes, err := encode(key, value, w.dataFile)
	if err != nil {
		return fmt.Errorf("failed to write to the data file: %w", err)
	}

	indexBytes, err := encodeKeyOffset(key, w.dataPos, w.indexFile)
	if err != nil {
		return fmt.Errorf("failed to write to the index file: %w", err)
	}

	if w.keyNum%w.sparseKeyDistance == 0 {
		if _, err := encodeKeyOffset(key, w.indexPos, w.sparseIndexFile); err != nil {
			return fmt.Errorf("failed to write to the file: %w", err)
		}
	}

	w.dataPos += dataBytes
	w.indexPos += indexBytes
	w.keyNum++

	return nil
}

// sync commits all written contents to the stable storage.
func (w *diskTableWriter) sync() error {
	if err := w.dataFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync data file: %w", err)
	}

	if err := w.indexFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync index file: %w", err)
	}

	if err := w.sparseIndexFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync sparse index file: %w", err)
	}

	return nil
}

// close closes all associated files with the disk table.
func (w *diskTableWriter) close() error {
	if err := w.dataFile.Close(); err != nil {
		return fmt.Errorf("failed to close data file: %w", err)
	}

	if err := w.indexFile.Close(); err != nil {
		return fmt.Errorf("failed to close index file: %w", err)
	}

	if err := w.sparseIndexFile.Close(); err != nil {
		return fmt.Errorf("failed to close sparse index file: %w", err)
	}

	return nil
}

// updateDiskTableMeta updates the current maximum disk table number.
func updateDiskTableMeta(dbDir string, num, max int) error {
	filePath := path.Join(dbDir, diskTableNumFileName)
	if err := ioutil.WriteFile(filePath, encodeIntPair(num, max), 0600); err != nil {
		return fmt.Errorf("failed to write %s: %w", filePath, err)
	}

	return nil
}

// readDiskTableMeta reads and returns the disk table num and the max index.
func readDiskTableMeta(dbDir string) (int, int, error) {
	filePath := path.Join(dbDir, diskTableNumFileName)
	data, err := ioutil.ReadFile(filePath)
	if err != nil && !os.IsNotExist(err) {
		return 0, 0, fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	if err != nil && os.IsNotExist(err) {
		return 0, -1, nil
	}

	num, max := decodeIntPair(data)

	return num, max, nil
}
