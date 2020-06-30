package lsmt

import (
	"testing"

	"github.com/patrickgombert/lsmt/common"
	"github.com/patrickgombert/lsmt/config"
)

var sink config.Level = config.Level{BlockSize: 100, SSTSize: 1000, BlockCacheSize: 1000}
var options config.Options = config.Options{Levels: []config.Level{sink}, KeyMaximumSize: 10, ValueMaximumSize: 10, Path: common.TEST_DIR}

func TestWriteNilOrEmptyKeyReturnsError(t *testing.T) {
	common.SetUp(t)

	lsmt, _ := Lsmt(options)
	err := lsmt.Write(nil, []byte{0})
	if err == nil {
		t.Error("Expected lsmt to error when writing nil key, but did not")
	}
	err = lsmt.Write([]byte{}, []byte{0})
	if err == nil {
		t.Error("Expected lsmt to error when writing empty key, but did not")
	}

	common.TearDown(t)
}

func TestWriteNilOrEmptyValueReturnsError(t *testing.T) {
	common.SetUp(t)

	lsmt, _ := Lsmt(options)
	err := lsmt.Write([]byte{0}, nil)
	if err == nil {
		t.Error("Expected lsmt to error when writing nil value, but did not")
	}
	err = lsmt.Write([]byte{0}, []byte{})
	if err == nil {
		t.Error("Expected lsmt to error when writing empty value, but did not")
	}

	common.TearDown(t)
}

func TestDeleteNilOrEmptyKeyReturnsError(t *testing.T) {
	common.SetUp(t)

	lsmt, _ := Lsmt(options)
	err := lsmt.Delete(nil)
	if err == nil {
		t.Error("Expected lsmt to error when deleting nil key, but did not")
	}
	err = lsmt.Delete([]byte{})
	if err == nil {
		t.Error("Expected lsmt to error when deleting empty key, but did not")
	}

	common.TearDown(t)
}

func TestIteratorStartNilOrEmptyReturnsError(t *testing.T) {
	common.SetUp(t)

	lsmt, _ := Lsmt(options)
	_, err := lsmt.Iterator(nil, []byte{1})
	if err == nil {
		t.Error("Expected lsmt to error when creating an iterator with a nil start value, but did not")
	}
	_, err = lsmt.Iterator([]byte{}, []byte{1})
	if err == nil {
		t.Error("Expected lsmt to error when creating an iterator with an empty start value, but did not")
	}

	common.TearDown(t)
}

func TestIteratorEndNilOrEmptyreturnsError(t *testing.T) {
	common.SetUp(t)

	lsmt, _ := Lsmt(options)
	_, err := lsmt.Iterator([]byte{1}, nil)
	if err == nil {
		t.Error("Expected lsmt to error when creating an iterator with a nil end value, but did not")
	}
	_, err = lsmt.Iterator([]byte{1}, []byte{})
	if err == nil {
		t.Error("Expected lsmt to error when creating an iterator with an empty end value, but did not")
	}

	common.TearDown(t)
}

func TestIteratorStartNotLessThanEnd(t *testing.T) {
	common.SetUp(t)

	lsmt, _ := Lsmt(options)
	_, err := lsmt.Iterator([]byte{1}, []byte{0})
	if err == nil {
		t.Error("Expected lsmt to error when the start key is not less than the end key when creating an iterator, but did not")
	}

	common.TearDown(t)
}
