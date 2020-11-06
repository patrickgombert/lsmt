package sst

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/patrickgombert/lsmt/common"
	c "github.com/patrickgombert/lsmt/comparator"
	"github.com/patrickgombert/lsmt/config"
)

type block struct {
	start  []byte
	end    []byte
	offset int64
}

type sst struct {
	file       string
	blocks     []*block
	metaOffset int64
}

type sstIterator struct {
	init       bool
	f          *os.File
	bytesRead  int64
	metaOffset int64
	key        []byte
	value      []byte
	start      []byte
	end        []byte
	closed     bool
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

func (sst *sst) ReadBlock(b *block, level config.Level) ([]byte, error) {
	f, err := os.Open(sst.file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	_, err = f.Seek(int64(b.offset), io.SeekStart)
	if err != nil {
		return nil, err
	}

	bytes := make([]byte, level.BlockSize)
	bytesRead, err := f.Read(bytes)
	if err != nil {
		return nil, err
	}

	return bytes[:bytesRead], nil
}

func (sst *sst) Iterator(start, end []byte) (*sstIterator, error) {
	f, err := os.Open(sst.file)
	if err != nil {
		return nil, err
	}

	startBlock := sst.blocks[0]
	for _, block := range sst.blocks[1:] {
		if c.Compare(start, block.start) == c.LESS_THAN {
			break
		}
		startBlock = block
	}

	f.Seek(startBlock.offset, io.SeekStart)
	bytesRead := startBlock.offset
	length := make([]byte, 1)
	var key []byte
	var value []byte
	for bytesRead < sst.metaOffset {
		_, err = f.Read(length)
		if err != nil {
			return nil, err
		}
		key = make([]byte, length[0])
		_, err = f.Read(key)
		if err != nil {
			return nil, err
		}
		_, err = f.Read(length)
		if err != nil {
			return nil, err
		}

		if c.Compare(start, key) == c.GREATER_THAN {
			f.Seek(int64(length[0]), io.SeekCurrent)
			bytesRead += int64(len(key) + int(length[0]) + 2)
		} else {
			value = make([]byte, length[0])
			_, err = f.Read(value)
			if err != nil {
				return nil, err
			}
			bytesRead += int64(len(key) + len(value) + 2)
			break
		}
	}

	if bytesRead >= sst.metaOffset {
		return &sstIterator{
			init:       false,
			f:          f,
			bytesRead:  sst.metaOffset,
			metaOffset: sst.metaOffset,
			key:        nil,
			value:      nil,
			start:      start,
			end:        end,
			closed:     true,
		}, nil
	} else {
		return &sstIterator{
			init:       false,
			f:          f,
			bytesRead:  bytesRead,
			metaOffset: sst.metaOffset,
			key:        key,
			value:      value,
			start:      start,
			end:        end,
			closed:     false,
		}, nil
	}
}

func (sst *sst) UnboundedIterator() (*sstIterator, error) {
	f, err := os.Open(sst.file)
	if err != nil {
		return nil, err
	}

	f.Seek(sst.blocks[0].offset, io.SeekStart)
	bytesRead := sst.blocks[0].offset
	length := make([]byte, 1)

	_, err = f.Read(length)
	if err != nil {
		return nil, err
	}
	key := make([]byte, length[0])
	_, err = f.Read(key)
	if err != nil {
		return nil, err
	}
	_, err = f.Read(length)
	if err != nil {
		return nil, err
	}
	value := make([]byte, length[0])
	_, err = f.Read(value)
	if err != nil {
		return nil, err
	}
	bytesRead += int64(len(key) + len(value) + 2)

	return &sstIterator{
		init:       false,
		f:          f,
		bytesRead:  bytesRead,
		metaOffset: sst.metaOffset,
		key:        key,
		value:      value,
		start:      nil,
		end:        nil,
		closed:     false,
	}, nil
}

func (iter *sstIterator) Next() (bool, error) {
	if iter.closed {
		return false, nil
	}

	if iter.bytesRead >= iter.metaOffset && iter.init {
		return false, nil
	}

	if !iter.init {
		iter.init = true
		return true, nil
	}

	length := make([]byte, 1)
	_, err := iter.f.Read(length)
	if err != nil {
		return false, err
	}
	key := make([]byte, length[0])
	_, err = iter.f.Read(key)
	if err != nil {
		return false, err
	}

	if iter.end != nil && c.Compare(key, iter.end) == c.GREATER_THAN {
		return false, nil
	}

	_, err = iter.f.Read(length)
	if err != nil {
		return false, err
	}
	value := make([]byte, length[0])
	_, err = iter.f.Read(value)
	if err != nil {
		return false, err
	}

	iter.key = key
	iter.value = value
	iter.bytesRead += int64(len(key) + len(value) + 2)
	return true, nil
}

func (iter *sstIterator) Get() (*common.Pair, error) {
	return &common.Pair{Key: iter.key, Value: iter.value}, nil
}

func (iter *sstIterator) Close() error {
	err := iter.f.Close()
	iter.closed = true
	return err
}

func Flush(options config.Options, level config.Level, iter common.Iterator) ([]*sst, error) {
	if iter == nil {
		return nil, errors.New("unable to flush nil iterator")
	}

	var err error
	var f *os.File
	var w *bufio.Writer

	ssts := []*sst{}
	var blocks []*block
	var currentBlock *block
	var previousPair *common.Pair

	bytesWritten := int64(0)
	currentBlockSize := int64(0)
	next, _ := iter.Next()

	for next {
		pair, _ := iter.Get()
		if c.Compare(pair.Value, common.Tombstone) != c.EQUAL {
			recordLength := int64(len(pair.Key) + len(pair.Value) + 2)

			if bytesWritten+recordLength > level.SSTSize {
				ssts[len(ssts)-1].metaOffset = bytesWritten
				currentBlock.end = previousPair.Key
				writeMeta(w, bytesWritten, blocks)
				f.Close()
				f = nil
			}

			if f == nil {
				f, err = newFile(options.Path)
				if err != nil {
					return nil, err
				}
				w = bufio.NewWriter(f)
				currentBlock = &block{start: pair.Key, offset: 0}
				blocks = []*block{currentBlock}
				ssts = append(ssts, &sst{file: f.Name(), blocks: blocks})
				bytesWritten = 0
				currentBlockSize = 0
			}

			if currentBlockSize+recordLength > level.BlockSize {
				currentBlock.end = previousPair.Key
				currentBlock = &block{start: pair.Key, offset: bytesWritten}
				currentBlockSize = 0
				blocks = append(blocks, currentBlock)
				w.Flush()
			}

			w.WriteByte(byte(len(pair.Key)))
			w.Write(pair.Key)
			w.WriteByte(byte(len(pair.Value)))
			w.Write(pair.Value)

			bytesWritten += recordLength
			currentBlockSize += recordLength
			previousPair = pair
		}
		next, _ = iter.Next()
	}

	if len(ssts) > 0 {
		ssts[len(ssts)-1].metaOffset = bytesWritten
		currentBlock.end = previousPair.Key
		writeMeta(w, bytesWritten, blocks)
		f.Close()
	}

	return ssts, nil
}

func OpenSst(path string) (*sst, error) {
	f, err := os.Open(path)
	if err != nil {
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
		offset := bytesToInt64(int64holder)
		block := &block{start: startKey, end: endKey, offset: offset}
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

func writeMeta(w *bufio.Writer, metaStart int64, blocks []*block) {
	w.Write(int64toBytes(int64(len(blocks))))
	for _, block := range blocks {
		w.WriteByte(byte(len(block.start)))
		w.Write(block.start)
		w.WriteByte(byte(len(block.end)))
		w.Write(block.end)
		w.Write(int64toBytes(block.offset))
	}
	w.Write(int64toBytes(metaStart))
	w.Flush()
}

func int64toBytes(i int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i))
	return b
}

func bytesToInt64(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}
