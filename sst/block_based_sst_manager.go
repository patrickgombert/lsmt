package sst

import (
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

	manager := &BlockBasedSSTManager{levels: levels, manifest: manifest}
	return manager, nil
}

func (manager *BlockBasedSSTManager) Get(key []byte) ([]byte, error) {
	//for _, level := manager.levels {
	//
	//}

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
