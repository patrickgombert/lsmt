package lsmt

import (
	"sync/atomic"
	"unsafe"

	"github.com/rs/zerolog/log"

	"github.com/patrickgombert/lsmt/common"
	c "github.com/patrickgombert/lsmt/comparator"
	"github.com/patrickgombert/lsmt/config"
	mt "github.com/patrickgombert/lsmt/memtable"
	"github.com/patrickgombert/lsmt/sst"
)

const (
	Lifecycle = "lifecycle"
	Action    = "action"
)

type lsmt struct {
	options           *config.Options
	activeMemtable    *mt.Memtable
	inactiveMemtables []*mt.Memtable
	sstManager        sst.SSTManager
	flushLock         common.Semaphore
	closed            bool
}

// Creates a new log-structured merge-tree in accordance with the options provided.
// If an existing lsmt exists at options.path then it will be opened, otherwise a new
// lsmt will be created.
func Lsmt(options *config.Options) (*lsmt, []error) {
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

	log.Info().
		Int("manifest_version", mostRecentManifest.Version).
		Str(Lifecycle, "open").
		Send()

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
		return common.ERR_LSMT_CLOSED
	}
	if key == nil || len(key) == 0 {
		return common.ERR_KEY_NIL_OR_EMPTY
	}
	if len(key) > db.options.KeyMaximumSize {
		return common.ERR_KEY_TOO_LARGE
	}
	if value == nil || len(value) == 0 {
		return common.ERR_VAL_NIL_OR_EMPTY
	}
	if len(value) > db.options.ValueMaximumSize {
		return common.ERR_VAL_TOO_LARGE
	}

	db.activeMemtable.Write(key, value)
	db.checkFlush()

	return nil
}

// Deletes a key/value pair.
func (db *lsmt) Delete(key []byte) error {
	if db.closed {
		return common.ERR_LSMT_CLOSED
	}
	if key == nil || len(key) == 0 {
		return common.ERR_KEY_NIL_OR_EMPTY
	}

	db.activeMemtable.Write(key, common.Tombstone)
	db.checkFlush()

	return nil
}

// Creates a bounded iterator bounded by the start and end inclusive.
func (db *lsmt) Iterator(start, end []byte) (common.Iterator, error) {
	if start == nil || len(start) == 0 {
		return nil, common.ERR_START_NIL_OR_EMPTY
	}
	if end == nil || len(end) == 0 {
		return nil, common.ERR_END_NIL_OR_EMPTY
	}
	if c.Compare(start, end) != c.LESS_THAN {
		return nil, common.ERR_START_GREATER_THAN_END
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

	return common.NewMergedIterator(iters, false), nil
}

// Close the lsmt. Failure to call this function before exiting the process might result
// data loss. All memtable will be force flushed to disk.
// Once Close() is invoked all writes will fail.
func (db *lsmt) Close() error {
	db.closed = true

	log.Info().
		Str(Lifecycle, "close").
		Send()

	for db.flushLock.IsLocked() {
	}

	tables := make([]*mt.Memtable, len(db.inactiveMemtables)+1)
	tables[0] = db.activeMemtable
	for i, inactive := range db.inactiveMemtables {
		tables[i+1] = inactive
	}

	hasDataToFlush := false
	for _, table := range tables {
		if table.Bytes() > 0 {
			hasDataToFlush = true
			break
		}
	}

	if hasDataToFlush {
		log.Info().
			Int64("active_memtable_bytes", db.activeMemtable.Bytes()).
			Int64("maximum_memtable_bytes", db.options.MemtableMaximumSize).
			Int("inactive_memtables", len(db.inactiveMemtables)).
			Str(Action, "flush").
			Msg("attempting to force flush memtables")

		_, err := db.sstManager.Flush(tables)

		if err != nil {
			log.Error().
				Err(err).
				Str(Action, "flush").
				Msg("failed to force flush on shutdown!")
		}

		return err
	}

	return nil
}

// Check to see if the active memtable is ready to be flushed to disk. If so,
// asynchronously flush to disk.
func (db *lsmt) checkFlush() {
	activeMemtableBytes := db.activeMemtable.Bytes()
	if (activeMemtableBytes > db.options.MemtableMaximumSize) && db.flushLock.TryLock() {
		newTable := mt.NewMemtable()
		unsafeActive := (*unsafe.Pointer)(unsafe.Pointer(&db.activeMemtable))
		active := atomic.SwapPointer(unsafeActive, unsafe.Pointer(newTable))
		db.inactiveMemtables = append(db.inactiveMemtables, (*mt.Memtable)(active))

		log.Info().
			Int64("active_memtable_bytes", activeMemtableBytes).
			Int64("maximum_memtable_bytes", db.options.MemtableMaximumSize).
			Str(Action, "flush").
			Msg("attempting to flush full memtable")

		go func() {
			newManager, err := db.sstManager.Flush(db.inactiveMemtables)
			if err == nil && newManager != nil {
				db.inactiveMemtables = []*mt.Memtable{}
				db.sstManager = newManager
			}

			if err != nil {
				log.Error().
					Str(Action, "flush").
					Err(err).
					Send()
			}

			db.flushLock.Unlock()
			log.Info().
				Str(Action, "flush").
				Msg("releasing flush lock")
		}()
	}
}
