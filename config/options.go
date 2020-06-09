package config

import (
	"errors"
	"fmt"
)

type Level struct {
	BlockSize        int64
	BlockCacheSize   int64
	BlockCacheShards int
	SSTSize          int64
	MaximumSSTFiles  int
}

type Options struct {
	Levels              []Level
	Path                string
	MemtableMaximumSize int64
	KeyMaximumSize      int
	ValueMaximumSize    int
}

// Validates that all of the fields contained with the Options are valid. Returns a list
// of errors. If there are no errors then the list will be empty.
func (options Options) Validate() []error {
	errs := []error{}
	if len(options.Levels) == 0 {
		errs = append(errs, errors.New("Must specify at least one sst level"))
	}

	for _, level := range options.Levels {
		if int(level.BlockSize) < options.KeyMaximumSize {
			errs = append(errs, fmt.Errorf("KeyMaximumSize %d is larger than the level's BlockSize %d", options.KeyMaximumSize, level.BlockSize))
		} else if int(level.BlockSize) < options.ValueMaximumSize {
			errs = append(errs, fmt.Errorf("ValueMaximumSize %d is larger than the level's BlockSize %d", options.ValueMaximumSize, level.BlockSize))
		}

		if level.BlockSize > level.BlockCacheSize {
			errs = append(errs, fmt.Errorf("BlockSize %d is larger than the level's BlockCacheSize %d", level.BlockSize, level.BlockCacheSize))
		}
	}

	return errs
}
