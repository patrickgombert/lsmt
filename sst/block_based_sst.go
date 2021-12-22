package sst

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/rs/zerolog/log"

	"github.com/patrickgombert/lsmt/common"
	c "github.com/patrickgombert/lsmt/comparator"
	"github.com/patrickgombert/lsmt/config"
)

type block struct {
	start     []byte
	end       []byte
	usedBytes int64
	offset    int64
}

type sst struct {
	file       string
	blocks     []*block
	metaOffset int64
}

func (block *block) Shard(numShards int) int {
	return int(block.offset) % numShards
}

func (sst *sst) Path() string {
	return sst.file
}

func (sst *sst) GetBlock(key []byte) *block {
	for _, block := range sst.blocks {
		start := c.Compare(key, block.start)
		if (start != c.LESS_THAN || start == c.EQUAL) && c.Compare(key, block.end) != c.GREATER_THAN {
			return block
		}
	}

	return nil
}

func (sst *sst) ReadBlock(b *block, level config.LevelOptions) ([]byte, error) {
	f, err := os.Open(sst.file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	_, err = f.Seek(b.offset, io.SeekStart)
	if err != nil {
		log.Error().
			Str("path", sst.file).
			Int64("block_offset", b.offset).
			Err(err).
			Msg("failed to seek to block")
		return nil, err
	}

	bytes := make([]byte, b.usedBytes)
	bytesRead, err := f.Read(bytes)
	if err == nil && int64(bytesRead) != b.usedBytes {
		err = common.ERR_BLOCK_UNDERFLOW
	}
	if err != nil {
		log.Error().
			Str("path", sst.file).
			Int64("block_offset", b.offset).
			Int64("block_size", level.GetBlockSize()).
			Int64("block_used_bytes", b.usedBytes).
			Err(err).
			Msg("failed to read block")
		return nil, err
	}

	return bytes, nil
}

// Creates a bloom filter for the keys in this SST
func (sst *sst) populateBloomFilter(size uint32) (*common.BloomFilter, error) {
	bloomFilter := common.NewBloomFilter(size)
	iter, err := sst.UnboundedIterator()
	if err != nil {
		return nil, err
	}

	next, err := iter.Next()
	if err != nil {
		return nil, err
	}

	for next {
		pair, err := iter.Get()
		if err != nil {
			return nil, err
		}

		bloomFilter.Insert(pair.Key)

		next, err = iter.Next()
		if err != nil {
			return nil, err
		}
	}

	return bloomFilter, nil
}

func OpenSst(path string) (*sst, error) {
	f, err := os.Open(path)
	if err != nil {
		log.Error().
			Str("path", path).
			Msg("failed to open SST file")
		return nil, err
	}
	defer f.Close()

	int64holder := make([]byte, 8)
	keyLength := make([]byte, 1)

	// Read metadata start relative to start of file
	f.Seek(-8, io.SeekEnd)
	f.Read(int64holder)
	metaOffset := bytesToInt64(int64holder)

	// Seek to metadata start
	f.Seek(metaOffset, io.SeekStart)
	// Read number of blocks
	f.Read(int64holder)
	numBlocks := bytesToInt64(int64holder)
	blocks := make([]*block, numBlocks)

	for i := int64(0); i < numBlocks; i++ {
		f.Read(keyLength)
		startKey := make([]byte, keyLength[0])
		f.Read(startKey)
		f.Read(keyLength)
		endKey := make([]byte, keyLength[0])
		f.Read(endKey)

		f.Read(int64holder)
		usedBytes := bytesToInt64(int64holder)
		f.Read(int64holder)
		offset := bytesToInt64(int64holder)
		block := &block{start: startKey, end: endKey, usedBytes: usedBytes, offset: offset}
		blocks[i] = block
	}

	opened := &sst{file: path, blocks: blocks, metaOffset: metaOffset}
	return opened, nil
}

func newFile(path string) (*os.File, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return nil, err
	}

	fileName := fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	return os.Create(fmt.Sprintf("%s%s.sst", path, fileName))
}

func int64toBytes(i int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i))
	return b
}

func bytesToInt64(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}
