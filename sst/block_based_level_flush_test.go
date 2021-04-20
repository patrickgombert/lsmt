package sst

import (
	"testing"

	"github.com/patrickgombert/lsmt/common"
	c "github.com/patrickgombert/lsmt/comparator"
	"github.com/patrickgombert/lsmt/config"
)

func TestSingleSSTFileFlush(t *testing.T) {
	common.SetUp(t)
	defer common.TearDown(t)

	sink := &config.Sink{BlockSize: 4, BlockCacheSize: 8192, BlockCacheShards: 1, SSTSize: 8, BloomFilterSize: 1024}
	options := &config.Options{Levels: common.EMPTY_LEVELS, Sink: sink, Path: common.TEST_DIR, MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}

	flush := newFlush(options, sink, NOMAX)
	accepting(flush, []byte{0}, []byte{0}, true, t)
	accepting(flush, []byte{1}, []byte{1}, true, t)
	ssts, err := flush.close()

	if err != nil {
		t.Errorf("Failed to close flush with error %v", err)
	}
	if len(ssts) != 1 {
		t.Errorf("Expected flush to produce 1 sst but found %d", len(ssts))
	}

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

func TestMultiSSTFileFlush(t *testing.T) {
	common.SetUp(t)
	defer common.TearDown(t)

	sink := &config.Sink{BlockSize: 8, BlockCacheSize: 8192, BlockCacheShards: 1, SSTSize: 16, BloomFilterSize: 1024}
	options := &config.Options{Levels: common.EMPTY_LEVELS, Sink: sink, Path: common.TEST_DIR, MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}

	flush := newFlush(options, sink, NOMAX)
	accepting(flush, []byte{0}, []byte{0}, true, t)
	accepting(flush, []byte{1}, []byte{1}, true, t)
	accepting(flush, []byte{2}, []byte{2}, true, t)
	accepting(flush, []byte{3}, []byte{3}, true, t)
	accepting(flush, []byte{4}, []byte{4}, true, t)
	accepting(flush, []byte{5}, []byte{5}, true, t)
	accepting(flush, []byte{6}, []byte{6}, true, t)
	accepting(flush, []byte{7}, []byte{7}, true, t)

	ssts, err := flush.close()

	if err != nil {
		t.Errorf("Failed to close flush with error %v", err)
	}
	if len(ssts) != 2 {
		t.Errorf("Expected flush to produce 2 sst but found %d", len(ssts))
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

func TestHitsMaxLevelSizeFlush(t *testing.T) {
	common.SetUp(t)
	defer common.TearDown(t)

	level := &config.Level{BlockSize: 8, BlockCacheSize: 8192, BlockCacheShards: 1, SSTSize: 8, MaximumSSTFiles: 1, BloomFilterSize: 1024}
	sink := &config.Sink{BlockSize: 8, BlockCacheSize: 8192, BlockCacheShards: 1, SSTSize: 16, BloomFilterSize: 1024}
	options := &config.Options{Levels: []*config.Level{level}, Sink: sink, Path: common.TEST_DIR, MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}

	flush := newFlush(options, level, 8)
	accepting(flush, []byte{0}, []byte{0}, true, t)
	accepting(flush, []byte{1}, []byte{1}, true, t)
	accepting(flush, []byte{2}, []byte{2}, false, t)

	ssts, err := flush.close()

	if err != nil {
		t.Errorf("Failed to close flush with error %v", err)
	}
	if len(ssts) != 1 {
		t.Errorf("Expected flush to produce 1 sst but found %d", len(ssts))
	}

	sst, _ := OpenSst(ssts[0].file)
	if sst.file != ssts[0].file {
		t.Errorf("Expected opened sst to have file path %s, but got %s", sst.file, ssts[0].file)
	}
	if len(sst.blocks) != 1 {
		t.Errorf("Expected opened sst to have 1 blocks, but got %d", len(sst.blocks))
	}
	if c.Compare([]byte{0}, sst.blocks[0].start) != c.EQUAL {
		t.Errorf("Expected opened sst block 0 to start at %q, but got %q", []byte{0}, sst.blocks[0].start)
	}
	if c.Compare([]byte{1}, sst.blocks[0].end) != c.EQUAL {
		t.Errorf("Expected opened sst block 0 to end at %q, but got %q", []byte{1}, sst.blocks[0].start)
	}
}

func accepting(flush *blockBasedLevelFlush, k []byte, v []byte, done bool, t *testing.T) {
	d, err := flush.accept(&common.Pair{Key: k, Value: v})
	if d != done {
		t.Errorf("Expected flush done to be %v but was %v", d, done)
	}
	if err != nil {
		t.Errorf("Failed to accept flush pair with error %v", err)
	}
}
