package sst

import (
	"testing"

	"github.com/patrickgombert/lsmt/common"
	c "github.com/patrickgombert/lsmt/comparator"
	"github.com/patrickgombert/lsmt/memtable"
)

func TestFlushNilMemtable(t *testing.T) {
	levels := []common.Level{common.Level{BlockSize: 4096, SSTSize: 524288000}}
	options := common.Options{Levels: levels, Path: "/tmp/lsmt/", MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}
	_, err := Flush(options, levels[0], nil)
	if err == nil {
		t.Error("Expected flushing nil memtable to produce an error but did not")
	}
}

func TestFlush(t *testing.T) {
	common.SetUp(t)

	mt := memtable.NewMemtable()
	mt.Write([]byte{1}, []byte{1})

	levels := []common.Level{common.Level{BlockSize: 4096, SSTSize: 524288000}}
	options := common.Options{Levels: levels, Path: common.TEST_DIR, MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}
	_, err := Flush(options, levels[0], mt)
	if err != nil {
		t.Errorf("Failed to flush memtable with error %q", err)
	}

	common.TearDown(t)
}

func TestOpenErrorFileNotFound(t *testing.T) {
	_, err := OpenSst("/not/real")
	if err == nil {
		t.Errorf("Expected opening non-existent SST to error but did not")
	}
}

func TestFlushAndOpen(t *testing.T) {
	common.SetUp(t)

	mt := memtable.NewMemtable()
	mt.Write([]byte{0}, []byte{0})
	mt.Write([]byte{1}, []byte{1})

	levels := []common.Level{common.Level{BlockSize: 4, SSTSize: 8}}
	options := common.Options{Levels: levels, Path: common.TEST_DIR, MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}
	ssts, _ := Flush(options, levels[0], mt)

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
	if c.Compare([]byte{1}, sst.blocks[1].start) != c.EQUAL {
		t.Errorf("Expected opened sst block 1 to start at %q, but got %q", []byte{1}, sst.blocks[1].start)
	}

	common.TearDown(t)
}

func TestFlushAndOpenMultiSSTFlush(t *testing.T) {
	common.SetUp(t)

	mt := memtable.NewMemtable()
	mt.Write([]byte{0}, []byte{0})
	mt.Write([]byte{1}, []byte{1})
	mt.Write([]byte{2}, []byte{2})
	mt.Write([]byte{3}, []byte{3})

	levels := []common.Level{common.Level{BlockSize: 4, SSTSize: 8}}
	options := common.Options{Levels: levels, Path: common.TEST_DIR, MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}
	ssts, _ := Flush(options, levels[0], mt)

	if len(ssts) != 2 {
		t.Errorf("Expected to flush %d tables, but flushed %d", 2, len(ssts))
	}

	sst0, _ := OpenSst(ssts[0].file)
	if c.Compare([]byte{0}, sst0.blocks[0].start) != c.EQUAL {
		t.Errorf("Expected opened sst block 0 to start at %q, but got %q", []byte{0}, sst0.blocks[0].start)
	}
	if c.Compare([]byte{1}, sst0.blocks[1].start) != c.EQUAL {
		t.Errorf("Expected opened sst block 1 to start at %q, but got %q", []byte{1}, sst0.blocks[1].start)
	}

	sst1, _ := OpenSst(ssts[1].file)
	if c.Compare([]byte{2}, sst1.blocks[0].start) != c.EQUAL {
		t.Errorf("Expected opened sst block 0 to start at %q, but got %q", []byte{2}, sst1.blocks[0].start)
	}
	if c.Compare([]byte{3}, sst1.blocks[1].start) != c.EQUAL {
		t.Errorf("Expected opened sst block 0 to start at %q, but got %q", []byte{3}, sst1.blocks[1].start)
	}

	common.TearDown(t)
}

func TestFlushAndGet(t *testing.T) {
	common.SetUp(t)

	mt := memtable.NewMemtable()
	mt.Write([]byte{0}, []byte{0})
	mt.Write([]byte{1}, []byte{1})

	levels := []common.Level{common.Level{BlockSize: 4, SSTSize: 8}}
	options := common.Options{Levels: levels, Path: common.TEST_DIR, MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}
	ssts, _ := Flush(options, levels[0], mt)

	sst, _ := OpenSst(ssts[0].file)
	value, _ := sst.Get([]byte{1})

	if c.Compare([]byte{1}, value) != c.EQUAL {
		t.Errorf("Expected sst get to produce %q, but got %q", []byte{1}, value)
	}

	common.TearDown(t)
}

func TestFlushAndGetNotFound(t *testing.T) {
	common.SetUp(t)

	mt := memtable.NewMemtable()
	mt.Write([]byte{0}, []byte{0})
	mt.Write([]byte{1}, []byte{1})

	levels := []common.Level{common.Level{BlockSize: 4, SSTSize: 8}}
	options := common.Options{Levels: levels, Path: common.TEST_DIR, MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}
	ssts, _ := Flush(options, levels[0], mt)

	sst, _ := OpenSst(ssts[0].file)
	value, _ := sst.Get([]byte{2})
	if value != nil {
		t.Error("Expected non-existent key to not be found")
	}

	common.TearDown(t)
}

func TestIterFromStartOfFile(t *testing.T) {
	common.SetUp(t)

	mt := memtable.NewMemtable()
	mt.Write([]byte{1}, []byte{1, 1})
	mt.Write([]byte{2}, []byte{2, 2})

	levels := []common.Level{common.Level{BlockSize: 4096, SSTSize: 524288000}}
	options := common.Options{Levels: levels, Path: common.TEST_DIR, MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}
	ssts, _ := Flush(options, levels[0], mt)

	sst, _ := OpenSst(ssts[0].file)
	iter, _ := sst.Iterator([]byte{0}, []byte{3})
	defer iter.Close()

	common.CompareNext(iter, true, t)
	common.CompareGet(iter, []byte{1}, []byte{1, 1}, t)
	common.CompareNext(iter, true, t)
	common.CompareGet(iter, []byte{2}, []byte{2, 2}, t)
	common.CompareNext(iter, false, t)

	common.TearDown(t)
}

func TestIterStartsMidBlock(t *testing.T) {
	common.SetUp(t)

	mt := memtable.NewMemtable()
	mt.Write([]byte{1}, []byte{1, 1})
	mt.Write([]byte{2}, []byte{2, 2})
	mt.Write([]byte{3}, []byte{3, 3})

	levels := []common.Level{common.Level{BlockSize: 4096, SSTSize: 524288000}}
	options := common.Options{Levels: levels, Path: common.TEST_DIR, MemtableMaximumSize: 1048576, KeyMaximumSize: 1024, ValueMaximumSize: 4096}
	ssts, _ := Flush(options, levels[0], mt)

	sst, _ := OpenSst(ssts[0].file)
	iter, _ := sst.Iterator([]byte{0}, []byte{2})
	defer iter.Close()

	common.CompareNext(iter, true, t)
	common.CompareGet(iter, []byte{1}, []byte{1, 1}, t)
	common.CompareNext(iter, true, t)
	common.CompareGet(iter, []byte{2}, []byte{2, 2}, t)
	common.CompareNext(iter, false, t)

	common.TearDown(t)
}
