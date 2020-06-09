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
	levels       []*blockBasedLevel
	stagedLevels []*blockBasedLevel
}

func OpenBlockBasedSSTManager(manifest *Manifest, options config.Options) (*BlockBasedSSTManager, error) {
	levels := make(map[int8]*blockBasedLevel)
	for _, entry := range manifest.Entries {
		l, present := levels[entry.Level]
		if !present {
			levelOptions := options.Levels[entry.Level]
			cache := cache.NewShardedLRUCache(levelOptions.BlockCacheShards, levelOptions.BlockCacheSize)
			l = &blockBasedLevel{ssts: []*sst{}, blockCache: cache}
		}

		sst, err := OpenSst(entry.Path)
		if err != nil {
			return nil, err
		}
		l.ssts = append(l.ssts, sst)
		levels[entry.Level] = l
	}

	blockLevels := make([]*blockBasedLevel, len(levels))
	for level, blockLevel := range levels {
		blockLevels[level] = blockLevel
	}

	manager := &BlockBasedSSTManager{levels: blockLevels, stagedLevels: []*blockBasedLevel{}}
	return manager, nil
}

func (manager *BlockBasedSSTManager) Get(key []byte) ([]byte, error) {
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

func (manager *BlockBasedSSTManager) Flush(options config.Options, level config.Level, mt *memtable.Memtable) {

}
