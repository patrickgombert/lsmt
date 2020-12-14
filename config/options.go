package config

import (
	"fmt"
)

// Configuration for a particular level in the LSMT.
type Level struct {
	BlockSize        int64
	BlockCacheSize   int64
	BlockCacheShards int
	SSTSize          int64
	MaximumSSTFiles  int
	BloomFilterSize  uint32
}

// Configuration for the sink level.
// The sink is a special case level that represents the bottom most level.
// The configuration is missing MaximumSSTFiles since there is no maximum.
type Sink struct {
	BlockSize        int64
	BlockCacheSize   int64
	BlockCacheShards int64
	SSTSize          int64
	BloomFilterSize  uint32
}

// Options for an LSMT.
// All size options are specified in bytes.
type Options struct {
	Levels              []Level
	Sink                Sink
	Path                string
	MemtableMaximumSize int64
	KeyMaximumSize      int
	ValueMaximumSize    int
}

// Validates that all of the fields contained with the Options are valid. Returns a list
// of errors. If there are no errors then the list will be empty.
func (options Options) Validate() []error {
	errs := []error{}

	if options.MemtableMaximumSize < 1 {
		errs = append(errs, fmt.Errorf("MemtbleMaximumSize %d must be greater than 0", options.MemtableMaximumSize))
	}

	if options.KeyMaximumSize < 1 {
		errs = append(errs, fmt.Errorf("KeyMaximumSize %d must be greater than 0", options.KeyMaximumSize))
	}

	if options.ValueMaximumSize < 1 {
		errs = append(errs, fmt.Errorf("ValueMaximumSize %d must be greater than 0", options.ValueMaximumSize))
	}

	for _, level := range options.Levels {
		errs = append(errs, level.validate(options)...)
	}

	errs = append(errs, options.Sink.validate(options)...)

	return errs
}

func (level Level) validate(options Options) []error {
	errs := []error{}

	if int(level.BlockSize) < options.KeyMaximumSize {
		errs = append(errs, fmt.Errorf("KeyMaximumSize %d is larger than the level's BlockSize %d", options.KeyMaximumSize, level.BlockSize))
	}
	if int(level.BlockSize) < options.ValueMaximumSize {
		errs = append(errs, fmt.Errorf("ValueMaximumSize %d is larger than the level's BlockSize %d", options.ValueMaximumSize, level.BlockSize))
	}

	if level.BlockSize > level.BlockCacheSize {
		errs = append(errs, fmt.Errorf("BlockSize %d is larger than the level's BlockCacheSize %d", level.BlockSize, level.BlockCacheSize))
	}

	if level.BloomFilterSize < 1 {
		errs = append(errs, fmt.Errorf("BloomFilterSize %d must be greater than 0", level.BloomFilterSize))
	}

	if level.MaximumSSTFiles < 1 {
		errs = append(errs, fmt.Errorf("MaximumSSTFiles %d must be greater than 0", level.MaximumSSTFiles))
	}

	return errs
}

func (sink Sink) validate(options Options) []error {
	errs := []error{}

	if int(sink.BlockSize) < options.KeyMaximumSize {
		errs = append(errs, fmt.Errorf("KeyMaximumSize %d is larger than the sink's BlockSize %d", options.KeyMaximumSize, sink.BlockSize))
	}
	if int(sink.BlockSize) < options.ValueMaximumSize {
		errs = append(errs, fmt.Errorf("ValueMaximumSize %d is larger than the sink's BlockSize %d", options.ValueMaximumSize, sink.BlockSize))
	}

	if sink.BlockSize > sink.BlockCacheSize {
		errs = append(errs, fmt.Errorf("BlockSize %d is larger than the sink's BlockCacheSize %d", sink.BlockSize, sink.BlockCacheSize))
	}

	if sink.BloomFilterSize < 1 {
		errs = append(errs, fmt.Errorf("BloomFilterSize %d must be greater than 0", sink.BloomFilterSize))
	}

	return errs
}
