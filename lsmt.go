package lsmt

import (
	"errors"

	"github.com/patrickgombert/lsmt/common"
	mt "github.com/patrickgombert/lsmt/memtable"
)

type lsmt struct {
	options           common.Options
	activeMemtable    *mt.Memtable
	inactiveMemtables []*mt.Memtable
}

func Lsmt(options common.Options) (*lsmt, error) {
	err := options.Validate()
	if err != nil {
		return nil, err
	}

	return &lsmt{options: options, activeMemtable: mt.NewMemtable(), inactiveMemtables: []*mt.Memtable{}}, nil
}

func (db *lsmt) Write(key, value []byte) error {
	if key == nil || len(key) == 0 {
		return errors.New("key must not be nil and must not be empty")
	}
	if len(key) > db.options.KeyMaximumSize {
		return errors.New("key must not be greater than the maximum key size")
	}
	if value == nil || len(value) == 0 {
		return errors.New("value must not be nil and must not not be empty")
	}
	if len(value) > db.options.ValueMaximumSize {
		return errors.New("value must not be greater than the maximum value size")
	}

	db.activeMemtable.Write(key, value)

	return nil
}

func (db *lsmt) Delete(key []byte) error {
	if key == nil || len(key) == 0 {
		return errors.New("key must not be nil and must not be empty")
	}

	db.activeMemtable.Write(key, common.Tombstone)

	return nil
}
