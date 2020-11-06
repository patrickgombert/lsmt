package config

import "testing"

func TestBlockSizeMustBeLessThanMaximumKeySize(t *testing.T) {
	level := Level{BlockSize: 2, SSTSize: 1000, BlockCacheSize: 1000, BloomFilterSize: 1000}
	options := Options{Levels: []Level{level}, KeyMaximumSize: 1, ValueMaximumSize: 100}

	err := options.Validate()
	if len(err) != 1 {
		t.Error("Expcted KeyMaximumSize greater than BlockSize to produce an error, but did not")
	}
}

func TestBlockSizeMustBeLessThanMaximumValueSize(t *testing.T) {
	level := Level{BlockSize: 2, SSTSize: 1000, BlockCacheSize: 1000, BloomFilterSize: 1000}
	options := Options{Levels: []Level{level}, KeyMaximumSize: 1000, ValueMaximumSize: 1}

	err := options.Validate()
	if len(err) != 1 {
		t.Error("Expcted ValueMaximumSize greater than BlockSize to produce an error, but did not")
	}
}

func TestBlockCacheSizeMustBeLargerThanBlockSize(t *testing.T) {
	level := Level{BlockSize: 2, SSTSize: 1000, BlockCacheSize: 1, BloomFilterSize: 1000}
	options := Options{Levels: []Level{level}, KeyMaximumSize: 1, ValueMaximumSize: 1}

	err := options.Validate()
	if len(err) != 1 {
		t.Error("Expected BlockCacheSize being less than BlockSize to produce an error, but did not")
	}
}

func TestBloomFilterSizeMustBeGreaterThan0(t *testing.T) {
	level := Level{BlockSize: 2, SSTSize: 1000, BlockCacheSize: 1000, BloomFilterSize: 0}
	options := Options{Levels: []Level{level}, KeyMaximumSize: 1, ValueMaximumSize: 1}

	err := options.Validate()
	if len(err) != 1 {
		t.Error("Expected BloomFilterSize being less than 1 to produce an error, but did not")
	}
}
