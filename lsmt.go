package lsmt

import (
	"errors"
	"fmt"
)

var Tombstone = []byte{}

type Level struct {
	blockSize int64
	sstSize   int64
}

type Options struct {
	levels              []Level
	path                string
	memtableMaximumSize int64
	keyMaximumSize      int
	valueMaximumSize    int
}

type lsmt struct {
	options           Options
	activeMemtable    *memtable
	inactiveMemtables []*memtable
}

type pair struct {
	key   []byte
	value []byte
}

type iterator interface {
	Next() (bool, error)
	Get() (*pair, error)
	Close() error
}

func Lsmt(options Options) (*lsmt, error) {
	err := options.validate()
	if err != nil {
		return nil, err
	}
	return &lsmt{options: options, activeMemtable: Memtable(), inactiveMemtables: []*memtable{}}, nil
}

func (options Options) validate() error {
	if len(options.levels) == 0 {
		return errors.New("must specify at least one sst level")
	}

	for _, level := range options.levels {
		if int(level.blockSize) < options.keyMaximumSize {
			fmt.Errorf("keyMaximumSize %q is larger than a level's blocksize %q", options.keyMaximumSize, level.blockSize)
		} else if int(level.blockSize) < options.valueMaximumSize {
			fmt.Errorf("valueMaximumSize %q is larger than a level's blocksize %q", options.valueMaximumSize, level.blockSize)
		}
	}

	return nil
}

func (db *lsmt) Write(key, value []byte) error {
	if key == nil || len(key) == 0 {
		return errors.New("key must not be nil and must not be empty")
	}
	if len(key) > db.options.keyMaximumSize {
		return errors.New("key must not be greater than the maximum key size")
	}
	if value == nil || len(value) == 0 {
		return errors.New("value must not be nil and must not not be empty")
	}
	if len(value) > db.options.valueMaximumSize {
		return errors.New("value must not be greater than the maximum value size")
	}

	db.activeMemtable.Write(key, value)

	return nil
}

func (db *lsmt) Delete(key []byte) error {
	if key == nil || len(key) == 0 {
		return errors.New("key must not be nil and must not be empty")
	}

	db.activeMemtable.Write(key, Tombstone)

	return nil
}
