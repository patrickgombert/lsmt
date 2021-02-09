package config

import "testing"

func TestValid(t *testing.T) {
	options := validOptions()

	err := options.Validate()
	if len(err) > 0 {
		t.Error("Expected valid Options to not produce error(s), but did")
	}

	level1 := &Level{BlockSize: 1000, SSTSize: 1000, BlockCacheSize: 5000, BloomFilterSize: 1000, MaximumSSTFiles: 100}
	level2 := &Level{BlockSize: 2000, SSTSize: 2000, BlockCacheSize: 8000, BloomFilterSize: 500, MaximumSSTFiles: 500}
	options.Levels = []*Level{level1, level2}
	err = options.Validate()
	if len(err) > 0 {
		t.Error("Expected valid Options to not produce error(s), but did")
	}
}

func TestInvalidTopLevelOptions(t *testing.T) {
	options := validOptions()
	options.MemtableMaximumSize = 0
	err := options.Validate()
	if len(err) != 1 {
		t.Error("Expcted MemtableMaximumSize less than 1 to produce an error, but did not")
	}

	options = validOptions()
	options.KeyMaximumSize = 0
	err = options.Validate()
	if len(err) != 1 {
		t.Error("Expcted KeyMaximumSize less than 1 to produce an error, but did not")
	}

	options = validOptions()
	options.ValueMaximumSize = 0
	err = options.Validate()
	if len(err) != 1 {
		t.Error("Expcted KeyMaximumSize less than 1 to produce an error, but did not")
	}
}

func TestBlockSizeMustBeLessThanMaximumKeySize(t *testing.T) {
	level := &Level{BlockSize: 2, SSTSize: 1000, BlockCacheSize: 1000, BloomFilterSize: 1000, MaximumSSTFiles: 100}
	options := validOptions()
	options.ValueMaximumSize = 2
	options.Levels = []*Level{level}

	err := options.Validate()
	if len(err) != 1 {
		t.Error("Expcted KeyMaximumSize greater than BlockSize to produce an error, but did not")
	}

	options = validOptions()
	options.ValueMaximumSize = 2
	options.Sink.BlockSize = 2

	err = options.Validate()
	if len(err) != 1 {
		t.Error("Expcted KeyMaximumSize greater than BlockSize to produce an error, but did not")
	}
}

func TestBlockSizeMustBeLessThanMaximumValueSize(t *testing.T) {
	level := &Level{BlockSize: 2, SSTSize: 1000, BlockCacheSize: 1000, BloomFilterSize: 1000, MaximumSSTFiles: 100}
	options := validOptions()
	options.KeyMaximumSize = 2
	options.Levels = []*Level{level}

	err := options.Validate()
	if len(err) != 1 {
		t.Error("Expcted ValueMaximumSize greater than BlockSize to produce an error, but did not")
	}

	options = validOptions()
	options.KeyMaximumSize = 2
	options.Sink.BlockSize = 2

	err = options.Validate()
	if len(err) != 1 {
		t.Error("Expcted ValueMaximumSize greater than BlockSize to produce an error, but did not")
	}
}

func TestBlockCacheSizeMustBeLargerThanBlockSize(t *testing.T) {
	level := &Level{BlockSize: 1000, SSTSize: 1000, BlockCacheSize: 1, BloomFilterSize: 1000, MaximumSSTFiles: 100}
	options := validOptions()
	options.Levels = []*Level{level}

	err := options.Validate()
	if len(err) != 1 {
		t.Error("Expected BlockCacheSize being less than BlockSize to produce an error, but did not")
	}

	options = validOptions()
	options.Sink.BlockSize = 1000
	options.Sink.BlockCacheSize = 1

	err = options.Validate()
	if len(err) != 1 {
		t.Error("Expected BlockCacheSize being less than BlockSize to produce an error, but did not")
	}
}

func TestBloomFilterSizeMustBeGreaterThan0(t *testing.T) {
	level := &Level{BlockSize: 1000, SSTSize: 1000, BlockCacheSize: 2000, BloomFilterSize: 0, MaximumSSTFiles: 100}
	options := validOptions()
	options.Levels = []*Level{level}

	err := options.Validate()
	if len(err) != 1 {
		t.Error("Expected BloomFilterSize being less than 1 to produce an error, but did not")
	}

	options = validOptions()
	options.Sink.BloomFilterSize = 0

	err = options.Validate()
	if len(err) != 1 {
		t.Error("Expected BloomFilterSize being less than 1 to produce an error, but did not")
	}
}

func validOptions() *Options {
	sink := &Sink{BlockSize: 100, SSTSize: 1000, BlockCacheSize: 200, BloomFilterSize: 1000}
	return &Options{Levels: []*Level{}, Sink: sink, KeyMaximumSize: 50, ValueMaximumSize: 50, MemtableMaximumSize: 1000}
}
