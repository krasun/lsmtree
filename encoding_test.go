package lsmtree

import (
	"bytes"
	"testing"
)

func TestEncodePut(t *testing.T) {
	buffer := &bytes.Buffer{}

	key := []byte{1, 2, 3}
	value := []byte{4, 5, 6}
	if _, err := encode(key, value, buffer); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// total = 14, key = 3, key and value
	expected := []byte{0, 0, 0, 0, 0, 0, 0, 14, 0, 0, 0, 0, 0, 0, 0, 3, 1, 2, 3, 4, 5, 6}
	if !bytes.Equal(expected, buffer.Bytes()) {
		t.Fatalf("failed to encoded key/value, expected %v, but received %v", expected, buffer.Bytes())
	}
}

func TestEncodeDelete(t *testing.T) {
	buffer := &bytes.Buffer{}

	key := []byte{1, 2, 3}
	if _, err := encode(key, nil, buffer); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// total = 11, key = 3, key and value
	expected := []byte{0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 3, 1, 2, 3}
	if !bytes.Equal(expected, buffer.Bytes()) {
		t.Fatalf("failed to encode key/value, expected %v, but received %v", expected, buffer.Bytes())
	}
}

func TestDecodePut(t *testing.T) {
	data := []byte{0, 0, 0, 0, 0, 0, 0, 14, 0, 0, 0, 0, 0, 0, 0, 3, 1, 2, 3, 4, 5, 6}
	buffer := bytes.NewBuffer(data)

	key, value, err := decode(buffer)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if !bytes.Equal([]byte{1, 2, 3}, key) {
		t.Fatalf("failed to decode key, expected %v, but received %v", []byte{1, 2, 3}, key)
	}

	if !bytes.Equal([]byte{4, 5, 6}, value) {
		t.Fatalf("failed to decode value, expected %v, but received %v", []byte{4, 5, 6}, value)
	}
}

func TestDecodeDelete(t *testing.T) {
	data := []byte{0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 3, 1, 2, 3}
	buffer := bytes.NewBuffer(data)

	key, value, err := decode(buffer)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if !bytes.Equal([]byte{1, 2, 3}, key) {
		t.Fatalf("failed to decode key, expected %v, but received %v", []byte{1, 2, 3}, key)
	}

	if value != nil {
		t.Fatalf("failed to decode value, expected nil, but received %v", value)
	}
}

func TestEncodePutDecode(t *testing.T) {
	buffer := &bytes.Buffer{}

	key := []byte{1, 2, 3}
	value := []byte{4, 5, 6}
	if _, err := encode(key, value, buffer); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	decodedKey, decodedValue, err := decode(buffer)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if !bytes.Equal(key, decodedKey) {
		t.Fatalf("failed to encode/decode key, expected %v, but received %v", key, decodedKey)
	}

	if !bytes.Equal(value, decodedValue) {
		t.Fatalf("failed to encode/decode value, expected %v, but received %v", value, decodedValue)
	}
}

func TestEncodeDeleteDecode(t *testing.T) {
	buffer := &bytes.Buffer{}

	key := []byte{1, 2, 3}
	if _, err := encode(key, nil, buffer); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	decodedKey, decodedValue, err := decode(buffer)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if !bytes.Equal(key, decodedKey) {
		t.Fatalf("failed to encode/decode key, expected %v, but received %v", key, decodedKey)
	}

	if nil != decodedValue {
		t.Fatalf("failed to encode/decode value, expected %v, but received %v", nil, decodedValue)
	}
}
