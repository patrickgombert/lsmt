package sst

import (
	"bytes"
	"io"
	"strconv"

	"github.com/patrickgombert/lsmt/cache"
	"github.com/patrickgombert/lsmt/common"
	c "github.com/patrickgombert/lsmt/comparator"
	"github.com/patrickgombert/lsmt/config"
	"github.com/patrickgombert/lsmt/memtable"
)

type blockBasedLevel struct {
	ssts       []*sst
	blockCache cache.Cache
}

// A manager for block based SSTs
type BlockBasedSSTManager struct {
	levels    []*blockBasedLevel
	options   config.Options
	flushLock common.Semaphore
	manifest  *Manifest
}

func OpenBlockBasedSSTManager(manifest *Manifest, options config.Options) (*BlockBasedSSTManager, error) {
	levels := make([]*blockBasedLevel, len(manifest.Levels))
	for i, entries := range manifest.Levels {
		levelOptions := options.Levels[i]
		cache := cache.NewShardedLRUCache(levelOptions.BlockCacheShards, levelOptions.BlockCacheSize)
		ssts := make([]*sst, len(entries))
		l := &blockBasedLevel{ssts: ssts, blockCache: cache}

		for idx, entry := range entries {
			sst, err := OpenSst(entry.Path)
			if err != nil {
				return nil, err
			}
			ssts[idx] = sst
		}

		levels[i] = l
	}

	manager := &BlockBasedSSTManager{levels: levels, options: options, flushLock: common.NewSemaphore(1), manifest: manifest}
	return manager, nil
}

// Gets a value for the given key.
// The value at the highest level will be returned. If no value is found then it will
// return nil. Uses the write through block cache while searching for a value.
func (manager *BlockBasedSSTManager) Get(key []byte) ([]byte, error) {
	for i, level := range manager.levels {
		for _, sst := range level.ssts {
			foundBlock := sst.GetBlock(key)
			if foundBlock != nil {
				b, err := level.blockCache.Get(foundBlock, func(bl cache.Shardable) ([]byte, error) {
					return sst.ReadBlock(bl.(*block), manager.options.Levels[i])
				})

				if err != nil {
					return nil, err
				}

				reader := bytes.NewReader(b)
				length := make([]byte, 1)
				for {
					_, err = reader.Read(length)
					if err == io.EOF {
						return nil, nil
					}
					if err != nil {
						return nil, err
					}
					k := make([]byte, length[0])
					bytesRead, err := reader.Read(k)
					if err == io.EOF || bytesRead < int(length[0]) {
						return nil, nil
					}
					_, err = reader.Read(length)

					if c.Compare(k, key) == c.EQUAL {
						v := make([]byte, length[0])
						bytesRead, err = reader.Read(v)
						if err == io.EOF || bytesRead < int(length[0]) {
							return nil, nil
						}
						if err != nil {
							return nil, err
						}

						return v, nil
					} else {
						_, err = reader.Seek(int64(length[0]), io.SeekCurrent)
						if err == io.EOF {
							return nil, nil
						}
						if err != nil {
							return nil, err
						}
					}
				}
			}
		}
	}

	return nil, nil
}

// Creates a block cached iterator for each level of SSTs. Combines each level's iterator
// into a MergedIterator.
func (manager *BlockBasedSSTManager) Iterator(start, end []byte) (common.Iterator, error) {
	iterators := make([]common.Iterator, len(manager.levels))
	for i, level := range manager.levels {
		levelConfig := manager.options.Levels[i]
		iter, err := NewCachedIterator(start, end, level.blockCache, level.ssts, levelConfig)
		if err != nil {
			return nil, err
		}
		iterators[i] = iter
	}

	mergedIterator := common.NewMergedIterator(iterators)
	return mergedIterator, nil
}

// Flush a memable to disk. Calling Flush will also trigger compaction.
// A lock is used to ensure that only one flush operation can happen at a time.
func (manager *BlockBasedSSTManager) Flush(tables []*memtable.Memtable) (SSTManager, error) {
	if manager.flushLock.TryLock() {
		defer manager.flushLock.Unlock()

		// this is naive, but check if we can optimistically fit on the first level and if
		// not then just flush to the second and so forth.
		iters := make([]common.Iterator, len(tables))
		levelOptions := manager.options.Levels[0]
		for i, mt := range tables {
			iters[i] = mt.UnboundedIterator()
		}
		if len(manager.levels) > 0 {
			level := manager.levels[0]
			levelIter, err := NewCachedUnboundedIterator(level.blockCache, level.ssts, levelOptions)
			if err != nil {
				return nil, err
			}
			iters = append(iters, levelIter)
		}
		mergedIter := common.NewMergedIterator(iters)

		ssts, err := Flush(manager.options, manager.options.Levels[0], mergedIter)
		if err != nil {
			return nil, err
		}

		cache := cache.NewShardedLRUCache(levelOptions.BlockCacheShards, levelOptions.BlockCacheSize)
		newLevel := &blockBasedLevel{ssts: ssts, blockCache: cache}

		// Create a new SSTManager
		newLevels := make([]*blockBasedLevel, len(manager.levels))
		copy(newLevels, manager.levels)
		if len(newLevels) > 0 {
			newLevels[0] = newLevel
		} else {
			newLevels = append(newLevels, newLevel)
		}
		manifestLevels := make([][]SST, len(newLevels))
		for i, l := range newLevels {
			innerLevel := make([]SST, len(l.ssts))
			for j, s := range l.ssts {
				innerLevel[j] = s
			}
			manifestLevels[i] = innerLevel
		}

		path := manager.options.Path + manifestPrefix + strconv.Itoa(manager.manifest.Version+1)
		err = WriteManifest(path, manifestLevels)
		if err != nil {
			return nil, err
		}
		newManifest, err := MostRecentManifest(manager.options.Path)
		if err != nil {
			return nil, err
		}

		sstManager := &BlockBasedSSTManager{levels: newLevels, options: manager.options, flushLock: manager.flushLock, manifest: newManifest}

		return sstManager, nil
	}

	return nil, nil
}
