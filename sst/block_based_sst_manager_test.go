package sst

import (
	"testing"

	"github.com/patrickgombert/lsmt/common"
	c "github.com/patrickgombert/lsmt/comparator"
	"github.com/patrickgombert/lsmt/config"
	"github.com/patrickgombert/lsmt/memtable"
)

func TestGetFoundKeyNotInCache(t *testing.T) {
	common.SetUp(t)
	defer common.TearDown(t)

	mt := memtable.NewMemtable()
	mt.Write([]byte{0}, []byte{0})
	levels := []config.Level{config.Level{BlockSize: 4, SSTSize: 8, BlockCacheSize: 4, BlockCacheShards: 1}}
	options := config.Options{Levels: levels, Path: common.TEST_DIR, MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}
	ssts, _ := Flush(options, levels[0], mt.UnboundedIterator())
	entry := Entry{Path: ssts[0].Path()}
	manifest := &Manifest{Levels: [][]Entry{[]Entry{entry}}}
	manager, _ := OpenBlockBasedSSTManager(manifest, options)

	value, _ := manager.Get([]byte{0})
	if c.Compare([]byte{0}, value) != c.EQUAL {
		t.Errorf("Expected mananger Get to produce %q, but got %q", []byte{0}, value)
	}
}

func TestGetFoundKeyInCache(t *testing.T) {
	common.SetUp(t)
	defer common.TearDown(t)

	mt := memtable.NewMemtable()
	mt.Write([]byte{0}, []byte{0})
	levels := []config.Level{config.Level{BlockSize: 4, SSTSize: 8, BlockCacheSize: 4, BlockCacheShards: 1, BloomFilterSize: 1000}}
	options := config.Options{Levels: levels, Path: common.TEST_DIR, MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}
	ssts, _ := Flush(options, levels[0], mt.UnboundedIterator())
	entry := Entry{Path: ssts[0].Path()}
	manifest := &Manifest{Levels: [][]Entry{[]Entry{entry}}}
	manager, _ := OpenBlockBasedSSTManager(manifest, options)

	value, _ := manager.Get([]byte{0})
	value, _ = manager.Get([]byte{0})

	if c.Compare([]byte{0}, value) != c.EQUAL {
		t.Errorf("Expected mananger Get to produce %q, but got %q", []byte{0}, value)
	}
}

func TestGetNotFound(t *testing.T) {
	common.SetUp(t)
	defer common.TearDown(t)

	mt := memtable.NewMemtable()
	mt.Write([]byte{0}, []byte{0})
	mt.Write([]byte{2}, []byte{2})
	levels := []config.Level{config.Level{BlockSize: 8, SSTSize: 8, BlockCacheSize: 8, BlockCacheShards: 1}}
	options := config.Options{Levels: levels, Path: common.TEST_DIR, MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}
	ssts, _ := Flush(options, levels[0], mt.UnboundedIterator())
	entry := Entry{Path: ssts[0].Path()}
	manifest := &Manifest{Levels: [][]Entry{[]Entry{entry}}}
	manager, _ := OpenBlockBasedSSTManager(manifest, options)

	value, _ := manager.Get([]byte{1})
	if value != nil {
		t.Errorf("Expected non-existent key to product nil value, but got %q", value)
	}
}
