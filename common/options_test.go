package common

import "testing"

func TestBlockSizeMustBeLessThanMaximumKeySize(t *testing.T) {
	level := Level{BlockSize: 2, SSTSize: 1000}
	options := Options{Levels: []Level{level}, KeyMaximumSize: 1, ValueMaximumSize: 100}

	err := options.Validate()
	if err == nil {
		t.Error("Expcted keyMaximumSize greater than blockSize to produce an error, but did not")
	}
}

func TestBlockSizeMustBeLessThanMaximumValueSize(t *testing.T) {
	level := Level{BlockSize: 2, SSTSize: 1000}
	options := Options{Levels: []Level{level}, KeyMaximumSize: 1000, ValueMaximumSize: 1}

	err := options.Validate()
	if err == nil {
		t.Error("Expcted valueMaximumSize greater than blockSize to produce an error, but did not")
	}
}
