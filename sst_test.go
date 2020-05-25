package lsmt

import "testing"

func TestFlushNilMemtable(t *testing.T) {
	level := Level{blockSize: 4096, sstSize: 524288000}
	options := Options{levels: []Level{level}, path: "/tmp/lsmt/", memtableMaximumSize: 1048576, keyMaximumSize: 1024, valueMaximumSize: 4096}
	_, err := Flush(options, level, nil)
	if err == nil {
		t.Error("Expected flushing nil memtable to produce an error but did not")
	}
}

func TestFlush(t *testing.T) {
	setUp(t)

	mt := Memtable()
	mt.Write([]byte{1}, []byte{1})

	level := Level{blockSize: 4096, sstSize: 524288000}
	options := Options{levels: []Level{level}, path: TEST_DIR, memtableMaximumSize: 1048576, keyMaximumSize: 1024, valueMaximumSize: 4096}
	_, err := Flush(options, level, mt)
	if err != nil {
		t.Errorf("Failed to flush memtable with error %q", err)
	}

	tearDown(t)
}

func TestOpenErrorFileNotFound(t *testing.T) {
	_, err := OpenSst("/not/real")
	if err == nil {
		t.Errorf("Expected opening non-existent SST to error but did not")
	}
}

func TestFlushAndOpen(t *testing.T) {
	setUp(t)

	mt := Memtable()
	mt.Write([]byte{0}, []byte{0})
	mt.Write([]byte{1}, []byte{1})

	level := Level{blockSize: 4, sstSize: 8}
	options := Options{levels: []Level{level}, path: TEST_DIR, memtableMaximumSize: 1048576, keyMaximumSize: 1024, valueMaximumSize: 4096}
	ssts, _ := Flush(options, level, mt)

	sst, _ := OpenSst(ssts[0].file)
	if sst.file != ssts[0].file {
		t.Errorf("Expected opened sst to have file path %s, but got %s", sst.file, ssts[0].file)
	}
	if len(sst.blocks) != 2 {
		t.Errorf("Expected opened sst to have 2 blocks, but got %d", len(sst.blocks))
	}
	if Compare([]byte{0}, sst.blocks[0].start) != EQUAL {
		t.Errorf("Expected opened sst block 0 to start at %q, but got %q", []byte{0}, sst.blocks[0].start)
	}
	if Compare([]byte{1}, sst.blocks[1].start) != EQUAL {
		t.Errorf("Expected opened sst block 1 to start at %q, but got %q", []byte{1}, sst.blocks[1].start)
	}

	tearDown(t)
}

func TestFlushAndOpenMultiSSTFlush(t *testing.T) {
	setUp(t)

	mt := Memtable()
	mt.Write([]byte{0}, []byte{0})
	mt.Write([]byte{1}, []byte{1})
	mt.Write([]byte{2}, []byte{2})
	mt.Write([]byte{3}, []byte{3})

	level := Level{blockSize: 4, sstSize: 8}
	options := Options{levels: []Level{level}, path: TEST_DIR, memtableMaximumSize: 1048576, keyMaximumSize: 1024, valueMaximumSize: 4096}
	ssts, _ := Flush(options, level, mt)

	if len(ssts) != 2 {
		t.Errorf("Expected to flush %d tables, but flushed %d", 2, len(ssts))
	}

	sst0, _ := OpenSst(ssts[0].file)
	if Compare([]byte{0}, sst0.blocks[0].start) != EQUAL {
		t.Errorf("Expected opened sst block 0 to start at %q, but got %q", []byte{0}, sst0.blocks[0].start)
	}
	if Compare([]byte{1}, sst0.blocks[1].start) != EQUAL {
		t.Errorf("Expected opened sst block 1 to start at %q, but got %q", []byte{1}, sst0.blocks[1].start)
	}

	sst1, _ := OpenSst(ssts[1].file)
	if Compare([]byte{2}, sst1.blocks[0].start) != EQUAL {
		t.Errorf("Expected opened sst block 0 to start at %q, but got %q", []byte{2}, sst1.blocks[0].start)
	}
	if Compare([]byte{3}, sst1.blocks[1].start) != EQUAL {
		t.Errorf("Expected opened sst block 0 to start at %q, but got %q", []byte{3}, sst1.blocks[1].start)
	}

	tearDown(t)
}

func TestFlushAndGet(t *testing.T) {
	setUp(t)

	mt := Memtable()
	mt.Write([]byte{0}, []byte{0})
	mt.Write([]byte{1}, []byte{1})

	level := Level{blockSize: 4, sstSize: 8}
	options := Options{levels: []Level{level}, path: TEST_DIR, memtableMaximumSize: 1048576, keyMaximumSize: 1024, valueMaximumSize: 4096}
	ssts, _ := Flush(options, level, mt)

	sst, _ := OpenSst(ssts[0].file)
	value, _ := sst.Get([]byte{1})

	if Compare([]byte{1}, value) != EQUAL {
		t.Errorf("Expected sst get to produce %q, but got %q", []byte{1}, value)
	}

	tearDown(t)
}

func TestFlushAndGetNotFound(t *testing.T) {
	setUp(t)

	mt := Memtable()
	mt.Write([]byte{0}, []byte{0})
	mt.Write([]byte{1}, []byte{1})

	level := Level{blockSize: 4, sstSize: 8}
	options := Options{levels: []Level{level}, path: TEST_DIR, memtableMaximumSize: 1048576, keyMaximumSize: 1024, valueMaximumSize: 4096}
	ssts, _ := Flush(options, level, mt)

	sst, _ := OpenSst(ssts[0].file)
	value, _ := sst.Get([]byte{2})
	if value != nil {
		t.Error("Expected non-existent key to not be found")
	}

	tearDown(t)
}

func TestIterFromStartOfFile(t *testing.T) {
	setUp(t)

	mt := Memtable()
	mt.Write([]byte{1}, []byte{1, 1})
	mt.Write([]byte{2}, []byte{2, 2})

	level := Level{blockSize: 4096, sstSize: 524288000}
	options := Options{levels: []Level{level}, path: TEST_DIR, memtableMaximumSize: 1048576, keyMaximumSize: 1024, valueMaximumSize: 4096}
	ssts, _ := Flush(options, level, mt)

	sst, _ := OpenSst(ssts[0].file)
	iter, _ := sst.Iterator([]byte{0}, []byte{3})
	defer iter.Close()

	compareNext(iter, true, t)
	compareGet(iter, []byte{1}, []byte{1, 1}, t)
	compareNext(iter, true, t)
	compareGet(iter, []byte{2}, []byte{2, 2}, t)
	compareNext(iter, false, t)

	tearDown(t)
}

func TestIterStartsMidBlock(t *testing.T) {
	setUp(t)

	mt := Memtable()
	mt.Write([]byte{1}, []byte{1, 1})
	mt.Write([]byte{2}, []byte{2, 2})
	mt.Write([]byte{3}, []byte{3, 3})

	level := Level{blockSize: 4096, sstSize: 524288000}
	options := Options{levels: []Level{level}, path: TEST_DIR, memtableMaximumSize: 1048576, keyMaximumSize: 1024, valueMaximumSize: 4096}
	ssts, _ := Flush(options, level, mt)

	sst, _ := OpenSst(ssts[0].file)
	iter, _ := sst.Iterator([]byte{0}, []byte{2})
	defer iter.Close()

	compareNext(iter, true, t)
	compareGet(iter, []byte{1}, []byte{1, 1}, t)
	compareNext(iter, true, t)
	compareGet(iter, []byte{2}, []byte{2, 2}, t)
	compareNext(iter, false, t)

	tearDown(t)
}
