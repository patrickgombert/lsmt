package sst

import (
	"github.com/patrickgombert/lsmt/common"
	"github.com/patrickgombert/lsmt/config"
	"github.com/patrickgombert/lsmt/memtable"
)

type MergedSST struct {
	path string
	err  error
}

type SST interface {
	Path() string
}

type SSTManager interface {
	Get(key []byte) ([]byte, error)
	Iterator(start, end []byte) (common.Iterator, error)
	Flush(options config.Options, mt *memtable.Memtable) chan MergedSST
}
