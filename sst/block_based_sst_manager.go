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
			bloomFilter := common.NewBloomFilter(level.GetBloomFilterSize())
			sst, err := OpenSst(entry.Path)
			if err != nil {
				return nil, err
			}
			ssts[idx] = sst

			iter, err := sst.UnboundedIterator()
			if err != nil {
				return nil, err
			}

			next, _ := iter.Next()
			for next {
				pair, err := iter.Get()
				if err == nil {
					bloomFilter.Insert(pair.Key)
				}
				next, _ = iter.Next()
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
		levelConfig := manager.options.Levels[i]
		iter, err := NewCachedIterator(start, end, level.blockCache, level.ssts, levelConfig)
		if err != nil {
			return nil, err
		}
		iterators[i] = iter
	}

	mergedIterator := common.NewMergedIterator(iterators, false)
	return mergedIterator, nil
}

// Flush a memable to disk. Calling Flush will also trigger compaction.
func (manager *BlockBasedSSTManager) Flush(tables []*memtable.Memtable) (SSTManager, error) {
	newManager := manager
	var err error
	for len(tables) > 0 {
		mtIters := []common.Iterator{}
		currentBytes := int64(0)
		levelBytes := manager.options.Levels[0].SSTSize * int64(manager.options.Levels[0].MaximumSSTFiles)
		i := 0
		for currentBytes <= levelBytes && i < len(tables) {
			currentBytes += tables[i].Bytes()
			mtIters = append(mtIters, tables[i].UnboundedIterator())
			i++
		}

		tables = tables[i:]

		iter := common.NewMergedIterator(mtIters, true)
		newManager, err = newManager.flushIter(iter, currentBytes)
		if err != nil {
			return nil, err
		}
	}
	return newManager, nil
}

func (manager *BlockBasedSSTManager) flushIter(iter common.Iterator, currentBytes int64) (*BlockBasedSSTManager, error) {
	// No compaction necessary if this is the first time flushing to disk
	if len(manager.levels) == 0 {
		ssts, err := Flush(manager.options, manager.options.Levels[0], iter)
		if err != nil {
			return nil, err
		}
		level := newLevel(ssts, manager.options.Levels[0])
		manifest, err := newManifest([]*blockBasedLevel{level}, manager.options.Path, manager.manifest.Version)
		if err != nil {
			return nil, err
		}
		newManager := &BlockBasedSSTManager{levels: []*blockBasedLevel{level}, options: manager.options, manifest: manifest}
		return newManager, nil
	} else {
		levels := manager.levels
		for i, levelOptions := range manager.options.Levels {
			var l *blockBasedLevel
			if i < len(manager.levels) {
				l = manager.levels[i]
			}
			// The remaining entries fit into the current level
			// or this is the final level (sink), so the flushing process can stop here
			if i == len(manager.options.Levels)-1 || l == nil || l.bytes(levelOptions)+currentBytes < levelOptions.SSTSize*int64(levelOptions.MaximumSSTFiles) {
				levelIter, err := NewCachedUnboundedIterator(l.blockCache, l.ssts, levelOptions)
				if err != nil {
					return nil, err
				}
				mergedIter := common.NewMergedIterator([]common.Iterator{iter, levelIter}, true)

				ssts, err := Flush(manager.options, levelOptions, mergedIter)
				if err != nil {
					return nil, err
				}

				level := newLevel(ssts, levelOptions)
				if i >= len(manager.levels) {
					levels = append(levels, level)
				} else {
					levels[i] = level
				}
				manifest, err := newManifest(levels, manager.options.Path, manager.manifest.Version)
				if err != nil {
					return nil, err
				}
				newManager := &BlockBasedSSTManager{levels: levels, options: manager.options, manifest: manifest}
				return newManager, nil
			} else {
				ssts, err := Flush(manager.options, levelOptions, iter)
				if err != nil {
					return nil, err
				}

				level := newLevel(ssts, levelOptions)
				levels[i] = level

				iter, err = NewCachedUnboundedIterator(l.blockCache, l.ssts, levelOptions)
				if err != nil {
					return nil, err
				}

				currentBytes = l.bytes(levelOptions)
			}
		}
		manifest, err := newManifest(levels, manager.options.Path, manager.manifest.Version)
		if err != nil {
			return nil, err
		}
		newManager := &BlockBasedSSTManager{levels: levels, options: manager.options, manifest: manifest}
		return newManager, nil
	}
}

func (level *blockBasedLevel) bytes(options config.LevelOptions) int64 {
	bytes := int64(0)
	for _, sst := range level.ssts {
		bytes += int64(len(sst.blocks)) * options.GetBlockSize()
	}
	return bytes
}

func newLevel(ssts []*sst, options config.LevelOptions) *blockBasedLevel {
	cache := cache.NewShardedLRUCache(options.GetBlockCacheShards(), options.GetBlockCacheSize())
	return &blockBasedLevel{ssts: ssts, blockCache: cache}
}

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
