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
				for {
					length := make([]byte, 1)
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

func (manager *BlockBasedSSTManager) Iterator(start, end []byte) (common.Iterator, error) {
	iterators := []common.Iterator{}
	for _, level := range manager.levels {
		for _, sst := range level.ssts {
			if c.Compare(end, sst.blocks[0].start) != c.LESS_THAN {
				iter, err := sst.Iterator(start, end)
				if err != nil {
					return nil, err
				}

				iterators = append(iterators, iter)
			}
		}
	}

	mergedIterator := common.NewMergedIterator(iterators)
	return mergedIterator, nil
}

func (manager *BlockBasedSSTManager) Flush(options config.Options, mt *memtable.Memtable) {
}
