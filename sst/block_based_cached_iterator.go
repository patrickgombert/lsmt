package sst

import (
	"bytes"
	"io"

	"github.com/patrickgombert/lsmt/cache"
	"github.com/patrickgombert/lsmt/common"
	c "github.com/patrickgombert/lsmt/comparator"
	"github.com/patrickgombert/lsmt/config"
)

// An iterator for a block based SST level.
type cachedIterator struct {
	end        []byte
	level      config.Level
	blockCache cache.Cache
	ssts       []*sst
	sstIndex   int
	block      *bytes.Reader
	blockIndex int
	nextKey    []byte
	nextValue  []byte
	closed     bool
}

// Returns a new iterator which uses the block cache when fetching blocks.
// Since the block cache and config are scoped to a level, the iterator also only
// iterates over a single level.
func NewCachedIterator(start, end []byte, blockCache cache.Cache, ssts []*sst, level config.Level) (common.Iterator, error) {
	for sstIndex, sst := range ssts {
		for blockIndex, bl := range sst.blocks {
			if c.Compare(start, bl.start) != c.LESS_THAN && c.Compare(start, bl.end) != c.GREATER_THAN {
				b, err := blockCache.Get(bl, func(arg cache.Shardable) ([]byte, error) {
					return sst.ReadBlock(arg.(*block), level)
				})
				if err != nil {
					return nil, err
				}

				reader := bytes.NewReader(b)
				length := make([]byte, 1)
				for {
					_, err = reader.Read(length)
					if err != nil {
						return nil, err
					}

					k := make([]byte, length[0])
					_, err = reader.Read(k)
					if err != nil {
						return nil, err
					}

					// If we've passed the start key, seek backwards as to start at the right position
					if c.Compare(start, k) != c.LESS_THAN {
						reader.Seek(int64(length[0])*-1, io.SeekCurrent)
						return &cachedIterator{end: end, level: level, blockCache: blockCache, ssts: ssts, sstIndex: sstIndex, block: reader, blockIndex: blockIndex, closed: false}, nil
						// Otherwise seek past the value
					} else {
						_, err = reader.Read(length)
						if err != nil {
							return nil, err
						}
						reader.Seek(int64(length[0]), io.SeekCurrent)
					}
				}
			}
		}
	}

	return &cachedIterator{closed: true}, nil
}

// Returns a new unbounded iterator which uses the block cache when fetching blocks.
// Since the block cache and config are scoped to a level, the iterator also only
// iterates over a single level.
func NewCachedUnboundedIterator(blockCache cache.Cache, ssts []*sst, level config.Level) (common.Iterator, error) {
	if len(ssts) == 0 {
		return &cachedIterator{closed: true}, nil
	}
	if len(ssts[0].blocks) == 0 {
		return &cachedIterator{closed: true}, nil
	}

	s := ssts[0]
	bl := s.blocks[0]
	b, err := blockCache.Get(bl, func(arg cache.Shardable) ([]byte, error) {
		return s.ReadBlock(arg.(*block), level)
	})
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(b)

	return &cachedIterator{end: nil, level: level, blockCache: blockCache, ssts: ssts, sstIndex: 0, block: reader, blockIndex: 0, closed: false}, nil
}

// Returns whether there is a next value. If necessary, calling Next will move the
// block index and block reader to the next block as well as move the sst index to the
// next SST.
func (iter *cachedIterator) Next() (bool, error) {
	if iter.closed {
		return false, nil
	}

	length := make([]byte, 1)
	_, err := iter.block.Read(length)
	// Move to the next block if possible when the end of a block is hit
	if err == io.EOF || length[0] == 0 {
		if iter.blockIndex == len(iter.ssts[iter.sstIndex].blocks)-1 {
			if iter.sstIndex == len(iter.ssts)-1 {
				iter.closed = true
				return false, nil
			} else {
				sst := iter.ssts[iter.sstIndex+1]
				bl := sst.blocks[0]
				b, err := iter.blockCache.Get(bl, func(arg cache.Shardable) ([]byte, error) {
					return sst.ReadBlock(arg.(*block), iter.level)
				})
				if err != nil {
					return false, err
				}
				iter.block = bytes.NewReader(b)
				iter.sstIndex++
				iter.blockIndex = 0
			}
		} else {
			sst := iter.ssts[iter.sstIndex]
			bl := sst.blocks[iter.blockIndex+1]
			b, err := iter.blockCache.Get(bl, func(arg cache.Shardable) ([]byte, error) {
				return sst.ReadBlock(arg.(*block), iter.level)
			})
			if err != nil {
				return false, err
			}
			iter.block = bytes.NewReader(b)
			iter.blockIndex++
		}
	}
	if err != nil {
		return false, err
	}

	k := make([]byte, length[0])
	_, err = iter.block.Read(k)
	if err != nil {
		return false, err
	}
	if iter.end != nil && c.Compare(k, iter.end) == c.GREATER_THAN {
		iter.closed = true
		return false, nil
	}

	_, err = iter.block.Read(length)
	if err != nil {
		return false, err
	}
	v := make([]byte, length[0])
	_, err = iter.block.Read(v)
	if err != nil {
		return false, err
	}
	iter.nextKey = k
	iter.nextValue = v

	return true, nil
}

// Get the current pair. The value is moved forward with each call to Next(). The value
// returned is cached and will continue returning the same value until Next() or Close()
// is invoked.
func (iter *cachedIterator) Get() (*common.Pair, error) {
	if iter.closed {
		return nil, nil
	}

	pair := &common.Pair{Key: iter.nextKey, Value: iter.nextValue}
	return pair, nil
}

// Closes the iterator. Will never throw an error.
func (iter *cachedIterator) Close() error {
	iter.closed = true
	return nil
}
