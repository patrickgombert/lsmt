package sst

import (
	"testing"

	"github.com/patrickgombert/lsmt/common"
	c "github.com/patrickgombert/lsmt/comparator"
	"github.com/patrickgombert/lsmt/config"
)

func TestOpenErrorFileNotFound(t *testing.T) {
	_, err := OpenSst("/not/real")
	if err == nil {
		t.Errorf("Expected opening non-existent SST to error but did not")
	}
}

func TestFlushAndOpen(t *testing.T) {
	common.SetUp(t)
	defer common.TearDown(t)

	sink := &config.Sink{BlockSize: 4, BlockCacheSize: 8192, BlockCacheShards: 1, SSTSize: 8, BloomFilterSize: 1024}
	options := &config.Options{Levels: common.EMPTY_LEVELS, Sink: sink, Path: common.TEST_DIR, MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}
	flush := newFlush(options, sink, NOMAX)
	flush.accept(&common.Pair{Key: []byte{0}, Value: []byte{0}})
	flush.accept(&common.Pair{Key: []byte{1}, Value: []byte{1}})
	ssts, _ := flush.close()

	sst, _ := OpenSst(ssts[0].file)
	if sst.file != ssts[0].file {
		t.Errorf("Expected opened sst to have file path %s, but got %s", sst.file, ssts[0].file)
	}
	if len(sst.blocks) != 2 {
		t.Errorf("Expected opened sst to have 2 blocks, but got %d", len(sst.blocks))
	}
	if c.Compare([]byte{0}, sst.blocks[0].start) != c.EQUAL {
		t.Errorf("Expected opened sst block 0 to start at %q, but got %q", []byte{0}, sst.blocks[0].start)
	}
	if c.Compare([]byte{0}, sst.blocks[0].end) != c.EQUAL {
		t.Errorf("Expected opened sst block 0 to end at %q, but got %q", []byte{0}, sst.blocks[0].start)
	}
	if c.Compare([]byte{1}, sst.blocks[1].start) != c.EQUAL {
		t.Errorf("Expected opened sst block 1 to start at %q, but got %q", []byte{1}, sst.blocks[1].start)
	}
	if c.Compare([]byte{1}, sst.blocks[1].end) != c.EQUAL {
		t.Errorf("Expected opened sst block 1 to end at %q, but got %q", []byte{1}, sst.blocks[1].start)
	}
}

func TestFlushAndOpenMultiSSTFlush(t *testing.T) {
	common.SetUp(t)
	defer common.TearDown(t)

	sink := &config.Sink{BlockSize: 8, BlockCacheSize: 8192, BlockCacheShards: 1, SSTSize: 16, BloomFilterSize: 1024}
	options := &config.Options{Levels: common.EMPTY_LEVELS, Sink: sink, Path: common.TEST_DIR, MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}
	flush := newFlush(options, sink, NOMAX)
	flush.accept(&common.Pair{Key: []byte{0}, Value: []byte{0}})
	flush.accept(&common.Pair{Key: []byte{1}, Value: []byte{1}})
	flush.accept(&common.Pair{Key: []byte{2}, Value: []byte{2}})
	flush.accept(&common.Pair{Key: []byte{3}, Value: []byte{3}})
	flush.accept(&common.Pair{Key: []byte{4}, Value: []byte{4}})
	flush.accept(&common.Pair{Key: []byte{5}, Value: []byte{5}})
	flush.accept(&common.Pair{Key: []byte{6}, Value: []byte{6}})
	flush.accept(&common.Pair{Key: []byte{7}, Value: []byte{7}})
	ssts, _ := flush.close()

	if len(ssts) != 2 {
		t.Errorf("Expected to flush %d tables, but flushed %d", 2, len(ssts))
	}

	sst0, _ := OpenSst(ssts[0].file)
	if len(sst0.blocks) != 2 {
		t.Errorf("Expected sst 0 to have %d blocks, but got %d", 2, len(sst0.blocks))
	}
	if c.Compare([]byte{0}, sst0.blocks[0].start) != c.EQUAL {
		t.Errorf("Expected opened sst block 0 to start at %q, but got %q", []byte{0}, sst0.blocks[0].start)
	}
	if c.Compare([]byte{1}, sst0.blocks[0].end) != c.EQUAL {
		t.Errorf("Expected opened sst block 0 to end at %q, but got %q", []byte{1}, sst0.blocks[0].end)
	}
	if c.Compare([]byte{2}, sst0.blocks[1].start) != c.EQUAL {
		t.Errorf("Expected opened sst block 1 to start at %q, but got %q", []byte{2}, sst0.blocks[1].start)
	}
	if c.Compare([]byte{3}, sst0.blocks[1].end) != c.EQUAL {
		t.Errorf("Expected opened sst block 1 to end at %q, but got %q", []byte{3}, sst0.blocks[1].end)
	}

	sst1, _ := OpenSst(ssts[1].file)
	if len(sst1.blocks) != 2 {
		t.Errorf("Expected sst 1 to have %d blocks, but got %d", 2, len(sst1.blocks))
	}
	if c.Compare([]byte{4}, sst1.blocks[0].start) != c.EQUAL {
		t.Errorf("Expected opened sst block 0 to start at %q, but got %q", []byte{4}, sst1.blocks[0].start)
	}
	if c.Compare([]byte{5}, sst1.blocks[0].end) != c.EQUAL {
		t.Errorf("Expected opened sst block 0 to end at %q, but got %q", []byte{5}, sst1.blocks[0].end)
	}
	if c.Compare([]byte{6}, sst1.blocks[1].start) != c.EQUAL {
		t.Errorf("Expected opened sst block 1 to start at %q, but got %q", []byte{6}, sst1.blocks[1].start)
	}
	if c.Compare([]byte{7}, sst1.blocks[1].end) != c.EQUAL {
		t.Errorf("Expected opened sst block 1 to end at %q, but got %q", []byte{7}, sst1.blocks[1].end)
	}
}
