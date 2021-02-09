package lsmt

import (
	"testing"

	"github.com/patrickgombert/lsmt/common"
	c "github.com/patrickgombert/lsmt/comparator"
	"github.com/patrickgombert/lsmt/config"
)

var sink *config.Sink = &config.Sink{BlockSize: 100, SSTSize: 1000, BlockCacheShards: 1, BlockCacheSize: 1000, BloomFilterSize: 1000}
var level *config.Level = &config.Level{BlockSize: 100, SSTSize: 1000, BlockCacheShards: 1, BlockCacheSize: 1000, BloomFilterSize: 1000, MaximumSSTFiles: 1}
var options *config.Options = &config.Options{Levels: []*config.Level{level}, Sink: sink, KeyMaximumSize: 10, ValueMaximumSize: 10, MemtableMaximumSize: 1000, Path: common.TEST_DIR}

func TestWriteNilOrEmptyKeyReturnsError(t *testing.T) {
	common.SetUp(t)
	defer common.TearDown(t)

	lsmt, errs := Lsmt(options)
	if errs != nil {
		t.Errorf("err %s\n", errs)
	}
	err := lsmt.Write(nil, []byte{0})
	if err == nil {
		t.Error("Expected lsmt to error when writing nil key, but did not")
	}
	err = lsmt.Write([]byte{}, []byte{0})
	if err == nil {
		t.Error("Expected lsmt to error when writing empty key, but did not")
	}
}

func TestWriteNilOrEmptyValueReturnsError(t *testing.T) {
	common.SetUp(t)
	defer common.TearDown(t)

	lsmt, _ := Lsmt(options)
	err := lsmt.Write([]byte{0}, nil)
	if err == nil {
		t.Error("Expected lsmt to error when writing nil value, but did not")
	}
	err = lsmt.Write([]byte{0}, []byte{})
	if err == nil {
		t.Error("Expected lsmt to error when writing empty value, but did not")
	}
}

func TestDeleteNilOrEmptyKeyReturnsError(t *testing.T) {
	common.SetUp(t)
	defer common.TearDown(t)

	lsmt, _ := Lsmt(options)
	err := lsmt.Delete(nil)
	if err == nil {
		t.Error("Expected lsmt to error when deleting nil key, but did not")
	}
	err = lsmt.Delete([]byte{})
	if err == nil {
		t.Error("Expected lsmt to error when deleting empty key, but did not")
	}
}

func TestIteratorStartNilOrEmptyReturnsError(t *testing.T) {
	common.SetUp(t)
	defer common.TearDown(t)

	lsmt, _ := Lsmt(options)
	_, err := lsmt.Iterator(nil, []byte{1})
	if err == nil {
		t.Error("Expected lsmt to error when creating an iterator with a nil start value, but did not")
	}
	_, err = lsmt.Iterator([]byte{}, []byte{1})
	if err == nil {
		t.Error("Expected lsmt to error when creating an iterator with an empty start value, but did not")
	}
}

func TestIteratorEndNilOrEmptyreturnsError(t *testing.T) {
	common.SetUp(t)
	defer common.TearDown(t)

	lsmt, _ := Lsmt(options)
	_, err := lsmt.Iterator([]byte{1}, nil)
	if err == nil {
		t.Error("Expected lsmt to error when creating an iterator with a nil end value, but did not")
	}
	_, err = lsmt.Iterator([]byte{1}, []byte{})
	if err == nil {
		t.Error("Expected lsmt to error when creating an iterator with an empty end value, but did not")
	}
}

func TestIteratorStartNotLessThanEnd(t *testing.T) {
	common.SetUp(t)
	defer common.TearDown(t)

	lsmt, _ := Lsmt(options)
	_, err := lsmt.Iterator([]byte{1}, []byte{0})
	if err == nil {
		t.Error("Expected lsmt to error when the start key is not less than the end key when creating an iterator, but did not")
	}
}

func TestDoesNotAcceptWritesAfterClose(t *testing.T) {
	common.SetUp(t)
	defer common.TearDown(t)

	lsmt, _ := Lsmt(options)
	lsmt.Close()

	err := lsmt.Write([]byte{0}, []byte{0})
	if err == nil {
		t.Error("Expected closed lsmt to not accept writes, but did")
	}
	err = lsmt.Delete([]byte{0})
	if err == nil {
		t.Error("Expected closed lsmt to not accept deletes, but did")
	}
}

func TestFlushWithoutExistingLevel(t *testing.T) {
	common.SetUp(t)
	defer common.TearDown(t)

	lsmt, _ := Lsmt(options)
	lsmt.Write([]byte{1, 1, 1, 1, 1, 1}, []byte{1, 1, 1, 1, 1, 1})
	lsmt.Close()

	openedLsmt, err := Lsmt(options)
	if err != nil {
		t.Errorf("Error opening lsmt %q", err)
	}
	result, _ := openedLsmt.Get([]byte{1, 1, 1, 1, 1, 1})
	if c.Compare(result, []byte{1, 1, 1, 1, 1, 1}) != c.EQUAL {
		t.Errorf("Expected opened lsmt to contain %q, but did not", []byte{1, 1, 1, 1, 1, 1})
	}
}

func TestFlushWithExistingLevel(t *testing.T) {
	common.SetUp(t)
	defer common.TearDown(t)

	lsmt, _ := Lsmt(options)
	lsmt.Write([]byte{1, 1, 1, 1, 1, 1}, []byte{1, 1, 1, 1, 1, 1})
	lsmt.Close()

	lsmt, _ = Lsmt(options)
	lsmt.Write([]byte{1, 0, 1}, []byte{1, 0, 1})
	lsmt.Close()

	openedLsmt, _ := Lsmt(options)
	result, _ := openedLsmt.Get([]byte{1, 1, 1, 1, 1, 1})
	if c.Compare(result, []byte{1, 1, 1, 1, 1, 1}) != c.EQUAL {
		t.Errorf("Expected opened lsmt to contain %q, but did not", []byte{1, 1, 1, 1, 1, 1})
	}

	result, _ = openedLsmt.Get([]byte{1, 0, 1})
	if c.Compare(result, []byte{1, 0, 1}) != c.EQUAL {
		t.Errorf("Expected opened lsmt to contain %q, but did not", []byte{1, 0, 1})
	}
}
