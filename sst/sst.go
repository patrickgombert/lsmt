package sst

import (
	"github.com/patrickgombert/lsmt/common"
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
	get(key []byte) ([]byte, error)
	iterator(start, end []byte) (*common.Iterator, error)
	flush(options common.Options, level common.Level, mt *memtable.Memtable) ([]*sst, error)
	merge(from common.Level, to common.Level) chan (MergedSST)
}
