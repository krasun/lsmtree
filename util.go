package main

import "bytes"

func deleteByKey(entries []entry, key []byte) []entry {
	b := entries[:0]
	for _, entry := range entries {
		if !bytes.Equal(entry.key, key) {
			b = append(b, entry)
		}
	}

	return b
}

func delete(slice []entry, i int) []entry {
	return append(slice[:i], slice[i+1:]...)
}
