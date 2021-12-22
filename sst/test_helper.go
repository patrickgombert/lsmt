package sst

import (
	"github.com/patrickgombert/lsmt/config"
	"github.com/patrickgombert/lsmt/memtable"
)

func FlushFrom(options *config.Options, table *memtable.Memtable) (SSTManager, error) {
	manifest := &Manifest{Levels: [][]Entry{}, Version: 0}
	sstManager, err := OpenBlockBasedSSTManager(manifest, options)
	if err != nil {
		return nil, err
	}
	return sstManager.Flush([]*memtable.Memtable{table})
}
