package common

import (
	"errors"
	"fmt"
)

type Level struct {
	BlockSize int64
	SSTSize   int64
}

type Options struct {
	Levels              []Level
	Path                string
	MemtableMaximumSize int64
	KeyMaximumSize      int
	ValueMaximumSize    int
}

func (options Options) Validate() error {
	if len(options.Levels) == 0 {
		return errors.New("must specify at least one sst level")
	}

	for _, level := range options.Levels {
		if int(level.BlockSize) < options.KeyMaximumSize {
			return fmt.Errorf("KeyMaximumSize %q is larger than a level's blocksize %q", options.KeyMaximumSize, level.BlockSize)
		} else if int(level.BlockSize) < options.ValueMaximumSize {
			return fmt.Errorf("ValueMaximumSize %q is larger than a level's blocksize %q", options.ValueMaximumSize, level.BlockSize)
		}
	}

	return nil
}
