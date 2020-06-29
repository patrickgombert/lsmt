package sst

import (
	"bytes"
	"io"

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
	levels   []*blockBasedLevel
	options  config.Options
	manifest *Manifest
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

	manager := &BlockBasedSSTManager{levels: levels, options: options, manifest: manifest}
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

func (manager *BlockBasedSSTManager) Flush(options config.Options, mt *memtable.Memtable) chan MergedSST {
	c := make(chan MergedSST)
	go func() {
		ssts, err := Flush(options, options.Levels[0], mt)
		if err != nil {
			c <- MergedSST{err: err}
		} else {
			for _, sst := range ssts {
				c <- MergedSST{path: sst.file}
			}
		}
	}()
	return c
}
