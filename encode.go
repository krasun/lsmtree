package lsmtree

import (
	"encoding/binary"
)

func encodePut(key []byte, value []byte) []byte {
	encodedKeyLen := encodeLen(len(key))
	encodedValueLen := encodeLen(len(value))
	len := len(encodedKeyLen) + len(key) + len(encodedValueLen) + len(value)
	data := make([]byte, 0, len)

	encodedLen := encodeLen(len)

	data = append(data, encodedLen...)
	data = append(data, encodedKeyLen...)
	data = append(data, key...)
	data = append(data, encodedValueLen...)
	data = append(data, value...)

	return data
}

func encodeLen(len int) []byte {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], uint64(len))

	return encoded[:]
}

func decodeLen(encoded []byte) int {
	return int(binary.LittleEndian.Uint64(encoded))
}

func decode(encoded []byte) ([]byte, []byte, bool) {
	keyLen := decodeLen(encoded[0:8])
	key := encoded[8 : 8+keyLen]
	keyPartLen := 8 + keyLen

	if keyPartLen+1 == len(encoded) {
		return key, nil, true
	}

	valueLenStart := 8 + keyLen
	value := encoded[valueLenStart+8:]

	return key, value, false
}

func encodeDelete(key []byte) []byte {
	encodedKeyLen := encodeLen(len(key))
	len := len(encodedKeyLen) + len(key) + 1
	data := make([]byte, 0, len)

	encodedLen := encodeLen(len)

	data = append(data, encodedLen...)
	data = append(data, encodedKeyLen...)
	data = append(data, key...)
	// mark deletion
	data = append(data, 0)

	return data
}
