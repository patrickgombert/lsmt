package lsmt

import (
	"os"
	"testing"
)

func TestFlushNilMemtable(t *testing.T) {
	level := Level{blockSize: 4096, sstSize: 524288000}
	options := Options{levels: []Level{level}, path: "/tmp/lsmt/", memtableMaximumSize: 1048576, keyMaximumSize: 1024, valueMaximumSize: 4096}
	_, err := Flush(options, level, nil)
	if err == nil {
		t.Error("Expected flushing nil memtable to produce an error but did not")
	}
}

func TestFlush(t *testing.T) {
	dir := "/tmp/lsmt"
	if os.Mkdir(dir, os.ModeDir|os.ModePerm) != nil {
		t.Errorf("Failed to setUp by creating directory: %s", dir)
	}

	mt := Memtable()
	mt.Write([]byte{1}, []byte{1})

	level := Level{blockSize: 4096, sstSize: 524288000}
	options := Options{levels: []Level{level}, path: "/tmp/lsmt/", memtableMaximumSize: 1048576, keyMaximumSize: 1024, valueMaximumSize: 4096}
	_, err := Flush(options, level, mt)
	if err != nil {
		t.Errorf("Failed to flush memtable with error %q", err)
	}

	if os.RemoveAll(dir) != nil {
		t.Errorf("Failed to tearDown by removing directory: %s", dir)
	}
}
