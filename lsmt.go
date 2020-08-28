package lsmt

import (
	"errors"
	"sync/atomic"
	"unsafe"

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
	flushLock         common.Semaphore
	closed            bool
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
		mostRecentManifest = &sst.Manifest{Levels: [][]sst.Entry{}, Version: 0}
	}
	sstManager, err := sst.OpenBlockBasedSSTManager(mostRecentManifest, options)
	if err != nil {
		return nil, []error{err}
	}

	return &lsmt{options: options, activeMemtable: mt.NewMemtable(), inactiveMemtables: []*mt.Memtable{}, sstManager: sstManager, flushLock: common.NewSemaphore(1), closed: false}, nil
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
	if db.closed {
		return errors.New("lsmt is closed")
	}
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
	db.checkFlush()

	return nil
}

// Deletes a key/value pair.
func (db *lsmt) Delete(key []byte) error {
	if db.closed {
		return errors.New("lsmt is closed")
	}
	if key == nil || len(key) == 0 {
		return errors.New("key must not be nil and must not be empty")
	}

	db.activeMemtable.Write(key, common.Tombstone)
	db.checkFlush()

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

// Close the lsmt. Failure to call this function before exiting the process might result
// data loss.
// Once Close() is invoked all writes will fail.
func (db *lsmt) Close() error {
	db.closed = true
	return db.forceFlush()
}

// Check to see if the active memtable is ready to be flushed to disk. If so,
// asynchronously flush to disk.
func (db *lsmt) checkFlush() {
	if (db.activeMemtable.Bytes() > db.options.MemtableMaximumSize) && db.flushLock.TryLock() {
		newTable := mt.NewMemtable()
		unsafeActive := (*unsafe.Pointer)(unsafe.Pointer(&db.activeMemtable))
		active := atomic.SwapPointer(unsafeActive, unsafe.Pointer(newTable))
		db.inactiveMemtables = append(db.inactiveMemtables, (*mt.Memtable)(active))
		go func() {
			newManager, err := db.sstManager.Flush(db.inactiveMemtables)
			if err == nil && newManager != nil {
				db.inactiveMemtables = []*mt.Memtable{}
				db.sstManager = newManager
			}
			db.flushLock.Unlock()
		}()
	}
}

// Synchronously force a flush to disk regardless of the size of the active memtable.
func (db *lsmt) forceFlush() error {
	for db.flushLock.IsLocked() {
	}
	tables := make([]*mt.Memtable, len(db.inactiveMemtables)+1)
	tables[0] = db.activeMemtable
	for i, inactive := range db.inactiveMemtables {
		tables[i+1] = inactive
	}
	_, err := db.sstManager.Flush(tables)
	return err
}
