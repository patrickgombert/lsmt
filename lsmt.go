package lsmt

import (
	"errors"
	"fmt"
)

type Level struct {
	blockSize uint64
	sstSize   uint64
}

type Options struct {
	levels              []Level
	path                string
	memtableMaximumSize uint64
	keyMaximumSize      uint64
	valueMaximumSize    uint64
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
		if level.blockSize < options.keyMaximumSize {
			fmt.Errorf("keyMaximumSize %q is larger than a level's blocksize %q", options.keyMaximumSize, level.blockSize)
		} else if level.blockSize < options.valueMaximumSize {
			fmt.Errorf("valueMaximumSize %q is larger than a level's blocksize %q", options.valueMaximumSize, level.blockSize)
		}
	}

	return nil
}

func (db *lsmt) Write(key, value []byte) error {
	if key == nil || len(key) == 0 {
		return errors.New("key must not be nil and must not be empty")
	}
	if value == nil || len(value) == 0 {
		return errors.New("value must not be nil and must not not be empty")
	}

	db.activeMemtable.Write(key, value)

	return nil
}

func (db *lsmt) Delete(key []byte) error {
	if key == nil || len(key) == 0 {
		return errors.New("key must not be nil and must not be empty")
	}

	db.activeMemtable.Write(key, []byte{})

	return nil
}
