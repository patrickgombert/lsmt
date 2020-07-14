package sst

import (
	"github.com/patrickgombert/lsmt/common"
	"github.com/patrickgombert/lsmt/memtable"
)

type SST interface {
	Path() string
}

type SSTManager interface {
	Get(key []byte) ([]byte, error)
	Iterator(start, end []byte) (common.Iterator, error)
	Flush(tables []*memtable.Memtable) (SSTManager, error)
}
