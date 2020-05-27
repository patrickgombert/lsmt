package lsmt

import (
	"testing"

	"github.com/patrickgombert/lsmt/common"
)

func TestWriteNilOrEmptyKeyReturnsError(t *testing.T) {
	levels := []common.Level{common.Level{BlockSize: 4000}}
	options := common.Options{Levels: levels, MemtableMaximumSize: 0}
	lsmt, _ := Lsmt(options)
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
	levels := []common.Level{common.Level{BlockSize: 4000}}
	options := common.Options{Levels: levels, MemtableMaximumSize: 0}
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
	levels := []common.Level{common.Level{BlockSize: 4000}}
	options := common.Options{Levels: levels, MemtableMaximumSize: 0}
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
