package sst

import (
	"testing"

	"github.com/patrickgombert/lsmt/common"
	c "github.com/patrickgombert/lsmt/comparator"
	"github.com/patrickgombert/lsmt/config"
	"github.com/patrickgombert/lsmt/memtable"
)

func TestFlushNilMemtable(t *testing.T) {
	levels := []config.Level{config.Level{BlockSize: 4096, SSTSize: 524288000}}
	options := config.Options{Levels: levels, Path: "/tmp/lsmt/", MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}
	_, err := Flush(options, levels[0], nil)
	if err == nil {
		t.Error("Expected flushing nil memtable to produce an error but did not")
	}
}

func TestFlush(t *testing.T) {
	common.SetUp(t)
	defer common.TearDown(t)

	mt := memtable.NewMemtable()
	mt.Write([]byte{1}, []byte{1})

	levels := []config.Level{config.Level{BlockSize: 4096, SSTSize: 524288000}}
	options := config.Options{Levels: levels, Path: common.TEST_DIR, MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}
	_, err := Flush(options, levels[0], mt.UnboundedIterator())
	if err != nil {
		t.Errorf("Failed to flush memtable with error %q", err)
	}
}

func TestOpenErrorFileNotFound(t *testing.T) {
	_, err := OpenSst("/not/real")
	if err == nil {
		t.Errorf("Expected opening non-existent SST to error but did not")
	}
}

func TestFlushAndOpen(t *testing.T) {
	common.SetUp(t)
	defer common.TearDown(t)

	mt := memtable.NewMemtable()
	mt.Write([]byte{0}, []byte{0})
	mt.Write([]byte{1}, []byte{1})

	levels := []config.Level{config.Level{BlockSize: 4, SSTSize: 8}}
	options := config.Options{Levels: levels, Path: common.TEST_DIR, MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}
	ssts, _ := Flush(options, levels[0], mt.UnboundedIterator())

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

	mt := memtable.NewMemtable()
	mt.Write([]byte{0}, []byte{0})
	mt.Write([]byte{1}, []byte{1})
	mt.Write([]byte{2}, []byte{2})
	mt.Write([]byte{3}, []byte{3})
	mt.Write([]byte{4}, []byte{4})
	mt.Write([]byte{5}, []byte{5})
	mt.Write([]byte{6}, []byte{6})
	mt.Write([]byte{7}, []byte{7})

	levels := []config.Level{config.Level{BlockSize: 8, SSTSize: 16}}
	options := config.Options{Levels: levels, Path: common.TEST_DIR, MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}
	ssts, _ := Flush(options, levels[0], mt.UnboundedIterator())

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

func TestIterFromStartOfFile(t *testing.T) {
	common.SetUp(t)
	defer common.TearDown(t)

	mt := memtable.NewMemtable()
	mt.Write([]byte{1}, []byte{1, 1})
	mt.Write([]byte{2}, []byte{2, 2})

	levels := []config.Level{config.Level{BlockSize: 4096, SSTSize: 524288000}}
	options := config.Options{Levels: levels, Path: common.TEST_DIR, MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}
	ssts, _ := Flush(options, levels[0], mt.UnboundedIterator())

	sst, _ := OpenSst(ssts[0].file)
	iter, _ := sst.Iterator([]byte{0}, []byte{3})
	defer iter.Close()

	common.CompareNext(iter, true, t)
	common.CompareGet(iter, []byte{1}, []byte{1, 1}, t)
	common.CompareNext(iter, true, t)
	common.CompareGet(iter, []byte{2}, []byte{2, 2}, t)
	common.CompareNext(iter, false, t)
}

func TestIterStartsMidBlock(t *testing.T) {
	common.SetUp(t)
	defer common.TearDown(t)

	mt := memtable.NewMemtable()
	mt.Write([]byte{1}, []byte{1, 1})
	mt.Write([]byte{2}, []byte{2, 2})
	mt.Write([]byte{3}, []byte{3, 3})

	levels := []config.Level{config.Level{BlockSize: 4096, SSTSize: 524288000}}
	options := config.Options{Levels: levels, Path: common.TEST_DIR, MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}
	ssts, _ := Flush(options, levels[0], mt.UnboundedIterator())

	sst, _ := OpenSst(ssts[0].file)
	iter, _ := sst.Iterator([]byte{0}, []byte{2})
	defer iter.Close()

	common.CompareNext(iter, true, t)
	common.CompareGet(iter, []byte{1}, []byte{1, 1}, t)
	common.CompareNext(iter, true, t)
	common.CompareGet(iter, []byte{2}, []byte{2, 2}, t)
	common.CompareNext(iter, false, t)
}
