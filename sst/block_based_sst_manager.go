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
	ssts         []*sst
	bloomFilters []*common.BloomFilter
	blockCache   cache.Cache
}

// A manager for block based SSTs
type BlockBasedSSTManager struct {
	levels   []*blockBasedLevel
	options  *config.Options
	manifest *Manifest
}

func OpenBlockBasedSSTManager(manifest *Manifest, options *config.Options) (*BlockBasedSSTManager, error) {
	levels := make([]*blockBasedLevel, len(manifest.Levels))
	levelOptions := make([]config.LevelOptions, len(manifest.Levels))
	for i, _ := range manifest.Levels {
		lvl, err := options.GetLevel(i)
		if err != nil {
			return nil, err
		}
		levelOptions[i] = lvl
	}

	for i, entries := range manifest.Levels {
		level := levelOptions[i]
		bloomFilters := make([]*common.BloomFilter, len(entries))
		cache := cache.NewShardedLRUCache(level.GetBlockCacheShards(), level.GetBlockCacheSize())
		ssts := make([]*sst, len(entries))
		l := &blockBasedLevel{ssts: ssts, bloomFilters: bloomFilters, blockCache: cache}

		for idx, entry := range entries {
			sst, err := OpenSst(entry.Path)
			if err != nil {
				return nil, err
			}
			ssts[idx] = sst
			bloomFilter, err := sst.populateBloomFilter(level.GetBloomFilterSize())
			if err != nil {
				return nil, err
			}
			bloomFilters[idx] = bloomFilter
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
	for _, level := range manager.levels {
		for i, sst := range level.ssts {
			if level.bloomFilters[i].Test(key) {
				foundBlock := sst.GetBlock(key)
				if foundBlock != nil {
					b, err := level.blockCache.Get(foundBlock, func(bl cache.Shardable) ([]byte, error) {
						var levelOptions, err = manager.options.GetLevel(i)
						if err != nil {
							return nil, err
						}
						return sst.ReadBlock(bl.(*block), levelOptions)
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
	}

	return nil, nil
}

// Creates a block cached iterator for each level of SSTs. Combines each level's iterator
// into a MergedIterator.
func (manager *BlockBasedSSTManager) Iterator(start, end []byte) (common.Iterator, error) {
	iterators := make([]common.Iterator, len(manager.levels))
	for i, level := range manager.levels {
		levelConfig, err := manager.options.GetLevel(i)
		if err != nil {
			return nil, err
		}
		iter, err := NewCachedIterator(start, end, level.blockCache, level.ssts, levelConfig)
		if err != nil {
			return nil, err
		}
		iterators[i] = iter
	}

	mergedIterator := common.NewMergedIterator(iterators, false)
	return mergedIterator, nil
}

// Flush a slice of memables to disk. Calling Flush will also trigger compaction.
func (manager *BlockBasedSSTManager) Flush(tables []*memtable.Memtable) (SSTManager, error) {
	iters := make([]common.Iterator, len(tables))
	for i, table := range tables {
		iters[i] = table.UnboundedIterator()
	}
	iter := common.NewMergedIterator(iters, true)

	newLevels := []*blockBasedLevel{}
	var pair *common.Pair

	for i, level := range manager.options.Levels {
		// Compose the new level into the existing iterator
		levelIter, err := manager.levelUnboundedIterator(i)
		if err != nil {
			return nil, err
		}
		iter = common.NewMergedIterator([]common.Iterator{iter, levelIter}, true)
		flush := newFlush(manager.options, level, level.SSTSize*int64(level.MaximumSSTFiles))

		for {
			// It is possible to have a leftover pair that was not accepted, check for that case first
			if pair != nil {
				accepted, err := flush.accept(pair)
				if err != nil {
					return nil, err
				}
				if !accepted {
					break
				}
			}

			next, err := iter.Next()
			if err != nil {
				return nil, err
			}
			// If the iterator has been exhausted then we are done
			if !next {
				ssts, err := flush.close()
				if err != nil {
					return nil, err
				}
				l, err := newLevel(ssts, level)
				if err != nil {
					return nil, err
				}
				newLevels = append(newLevels, l)

				manifest, err := newManifest(newLevels, manager.options.Path, manager.manifest.Version)
				if err != nil {
					return nil, err
				}
				newManager := &BlockBasedSSTManager{levels: newLevels, options: manager.options, manifest: manifest}
				return newManager, nil
			}
			pair, err = iter.Get()
			if err != nil {
				return nil, err
			}
		}

		// Close the flush and generate the new level
		ssts, err := flush.close()
		if err != nil {
			return nil, err
		}
		l, err := newLevel(ssts, level)
		if err != nil {
			return nil, err
		}
		newLevels = append(newLevels, l)
	}

	sinkIter, err := manager.levelUnboundedIterator(len(manager.options.Levels))
	if err != nil {
		return nil, err
	}
	iter = common.NewMergedIterator([]common.Iterator{iter, sinkIter}, false)
	flush := newFlush(manager.options, manager.options.Sink, NOMAX)

	for {
		// It is possible to have a leftover pair that was not accepted, check for that case first
		if pair != nil {
			accepted, err := flush.accept(pair)
			if err != nil {
				return nil, err
			}
			if !accepted {
				break
			}
		}

		next, err := iter.Next()
		if err != nil {
			return nil, err
		}
		// If the iterator has been exhausted then we are done
		if !next {
			break
		}
		pair, err = iter.Get()
		if err != nil {
			return nil, err
		}
	}

	ssts, err := flush.close()
	if err != nil {
		return nil, err
	}
	l, err := newLevel(ssts, manager.options.Sink)
	if err != nil {
		return nil, err
	}
	newLevels = append(newLevels, l)

	manifest, err := newManifest(newLevels, manager.options.Path, manager.manifest.Version)
	if err != nil {
		return nil, err
	}
	newManager := &BlockBasedSSTManager{levels: newLevels, options: manager.options, manifest: manifest}
	return newManager, nil
}

// Creates an unbounded cached iterator for a single level
func (manager *BlockBasedSSTManager) levelUnboundedIterator(level int) (common.Iterator, error) {
	levelConfig, err := manager.options.GetLevel(level)
	if err != nil {
		return nil, err
	}
	if level < len(manager.levels) {
		l := manager.levels[level]
		return NewCachedUnboundedIterator(l.blockCache, l.ssts, levelConfig)
	} else {
		return common.EmptyIterator(), nil
	}
}

// Creates a new blockBasedLevel
func newLevel(ssts []*sst, options config.LevelOptions) (*blockBasedLevel, error) {
	cache := cache.NewShardedLRUCache(options.GetBlockCacheShards(), options.GetBlockCacheSize())
	bloomFilters := make([]*common.BloomFilter, len(ssts))
	for i, sst := range ssts {
		bloomFilter, err := sst.populateBloomFilter(options.GetBloomFilterSize())
		if err != nil {
			return nil, err
		}
		bloomFilters[i] = bloomFilter
	}
	return &blockBasedLevel{ssts: ssts, bloomFilters: bloomFilters, blockCache: cache}, nil
}

// Creates a new Manifest
func newManifest(levels []*blockBasedLevel, path string, version int) (*Manifest, error) {
	manifestLevels := make([][]SST, len(levels))
	for i, l := range levels {
		innerLevel := make([]SST, len(l.ssts))
		for j, s := range l.ssts {
			innerLevel[j] = s
		}
		manifestLevels[i] = innerLevel
	}

	manifestPath := path + manifestPrefix + strconv.Itoa(version+1)
	err := WriteManifest(manifestPath, manifestLevels)
	if err != nil {
		return nil, err
	}
	return MostRecentManifest(path)
}
