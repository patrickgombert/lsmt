package lsmt

import (
	"errors"

	"github.com/patrickgombert/lsmt/common"
	c "github.com/patrickgombert/lsmt/comparator"
	"github.com/patrickgombert/lsmt/config"
	mt "github.com/patrickgombert/lsmt/memtable"
	"github.com/patrickgombert/lsmt/sst"
)

type lsmt struct {
	options           config.Options
	activeMemtable    *mt.Memtable
	inactiveMemtables []*mt.Memtable
	sstManager        sst.SSTManager
}

// Creates a new log-structured merge-tree in accordance with the options provided.
// If an existing lsmt exists at options.path then it will be opened, otherwise a new
// lsmt will be created.
func Lsmt(options config.Options) (*lsmt, []error) {
	errs := options.Validate()
	if len(errs) != 0 {
		return nil, errs
	}

	mostRecentManifest, err := sst.MostRecentManifest(options.Path)
	if err != nil {
		return nil, []error{err}
	}
	if mostRecentManifest == nil {
		mostRecentManifest = &sst.Manifest{Levels: [][]sst.Entry{}}
	}
	sstManager, err := sst.OpenBlockBasedSSTManager(mostRecentManifest, options)
	if err != nil {
		return nil, []error{err}
	}

	return &lsmt{options: options, activeMemtable: mt.NewMemtable(), inactiveMemtables: []*mt.Memtable{}, sstManager: sstManager}, nil
}

// Get the value for a given key. If the key does not exist then the value will be nil.
func (db *lsmt) Get(key []byte) ([]byte, error) {
	value, found := db.activeMemtable.Get(key)
	if found {
		if c.Compare(value, common.Tombstone) == c.EQUAL {
			return nil, nil
		} else {
			return value, nil
		}
	}
	for _, mt := range db.inactiveMemtables {
		value, found = mt.Get(key)
		if found {
			if c.Compare(value, common.Tombstone) == c.EQUAL {
				return nil, nil
			} else {
				return value, nil
			}
		}
	}

	v, err := db.sstManager.Get(key)
	if err != nil {
		return nil, err
	}
	if v != nil && c.Compare(v, common.Tombstone) != c.EQUAL {
		return v, nil
	}

	return nil, nil
}

// Write a key/value pair. If an error is returned then the key/value pair will not have
// been written.
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

// Deletes a key/value pair.
func (db *lsmt) Delete(key []byte) error {
	if key == nil || len(key) == 0 {
		return errors.New("key must not be nil and must not be empty")
	}

	db.activeMemtable.Write(key, common.Tombstone)

	return nil
}

// Creates a bounded iterator bounded by the start and end inclusive.
func (db *lsmt) Iterator(start, end []byte) (common.Iterator, error) {
	if start == nil || len(start) == 0 {
		return nil, errors.New("start must not be nil and must not be empty")
	}
	if end == nil || len(end) == 0 {
		return nil, errors.New("end must not be nil and must not be empty")
	}
	if c.Compare(start, end) != c.LESS_THAN {
		return nil, errors.New("start must be less than end")
	}

	memtable := db.activeMemtable
	inactive := db.inactiveMemtables
	iters := make([]common.Iterator, 2+len(inactive))
	iters[0] = memtable.Iterator(start, end)
	for i, inactiveMt := range inactive {
		iters[i+1] = inactiveMt.Iterator(start, end)
	}
	sstIter, err := db.sstManager.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	iters[len(iters)-1] = sstIter

	return common.NewMergedIterator(iters), nil
}
