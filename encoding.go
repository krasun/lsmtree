package lsmtree

import (
	"encoding/binary"
	"fmt"
	"io"
)

// encode encodes key and value and writes it to the specified writer.
// Returns the number of bytes written and error if occurred.
// The function must be compatible with decode: encode(decode(v)) == v.
func encode(key []byte, value []byte, w io.Writer) (int, error) {
	// encoding format:
	// [encoded total length in bytes][encoded key length in bytes][key][value]

	// number of bytes written
	bytes := 0

	keyLen := encodeInt(len(key))
	len := len(keyLen) + len(key) + len(value)
	encodedLen := encodeInt(len)

	if n, err := w.Write(encodedLen); err != nil {
		return n, err
	} else {
		bytes += n
	}

	if n, err := w.Write(keyLen); err != nil {
		return bytes + n, err
	} else {
		bytes += n
	}

	if n, err := w.Write(key); err != nil {
		return bytes + n, err
	} else {
		bytes += n
	}

	if n, err := w.Write(value); err != nil {
		return bytes + n, err
	} else {
		bytes += n
	}

	return bytes, nil
}

// decode decodes key and value by reading from the specified reader.
// Returns the number of bytes read and error if occurred.
// The function must be compatible with encode: encode(decode(v)) == v.
func decode(r io.Reader) ([]byte, []byte, error) {
	// encoding format:
	// [encoded total length in bytes][encoded key length in bytes][key][value]

	var encodedEntryLen [8]byte
	if _, err := r.Read(encodedEntryLen[:]); err != nil {
		return nil, nil, err
	}

	entryLen := decodeInt(encodedEntryLen[:])
	encodedEntry := make([]byte, entryLen)
	n, err := r.Read(encodedEntry)
	if err != nil {
		return nil, nil, err
	}

	if n < entryLen {
		return nil, nil, fmt.Errorf("the file is corrupted, failed to read entry")
	}

	keyLen := decodeInt(encodedEntry[0:8])
	key := encodedEntry[8 : 8+keyLen]
	keyPartLen := 8 + keyLen

	if keyPartLen == len(encodedEntry) {
		return key, nil, err
	}

	valueStart := keyPartLen
	value := encodedEntry[valueStart:]

	return key, value, err
}

// encodeKeyOffset encodes key offset and writes it to the given writer.
func encodeKeyOffset(key []byte, offset int, w io.Writer) (int, error) {
	return encode(key, encodeInt(offset), w)
}

// encodeInt encodes the int as a slice of bytes.
// Must be compatible with decodeInt.
func encodeInt(x int) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(x))

	return encoded[:]
}

// decodeInt decodes the slice of bytes as an int.
// Must be compatible with encodeInt.
func decodeInt(encoded []byte) int {
	return int(binary.BigEndian.Uint64(encoded))
}

// encodeIntPair encodes two ints.
func encodeIntPair(x, y int) []byte {
	var encoded [16]byte
	binary.BigEndian.PutUint64(encoded[0:8], uint64(x))
	binary.BigEndian.PutUint64(encoded[8:], uint64(y))

	return encoded[:]
}

// decodeIntPair decodes two ints.
func decodeIntPair(encoded []byte) (int, int) {
	x := int(binary.BigEndian.Uint64(encoded[0:8]))
	y := int(binary.BigEndian.Uint64(encoded[8:]))

	return x, y
}
