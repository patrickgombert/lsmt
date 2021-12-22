package sst

import (
	"bytes"
	"io"
	"os"

	"github.com/patrickgombert/lsmt/common"
)

// An unbounded Iterator which will traverse an entire SST file
type unboundedSstIterator struct {
	sst         *sst
	f           *os.File
	blockBuffer *bytes.Reader
	blockIndex  int
	closed      bool
	nextPair    *common.Pair
}

// Create a new unbounded iterator for the sst
func (sst *sst) UnboundedIterator() (*unboundedSstIterator, error) {
	f, err := os.Open(sst.file)
	if err != nil {
		return nil, err
	}

	f.Seek(sst.blocks[0].offset, io.SeekStart)
	blockBytes := make([]byte, sst.blocks[0].usedBytes)
	bytesRead, err := f.Read(blockBytes)
	if err == nil && int64(bytesRead) != sst.blocks[0].usedBytes {
		err = common.ERR_BLOCK_UNDERFLOW
	}
	if err != nil {
		return nil, err
	}
	blockBuffer := bytes.NewReader(blockBytes)

	return &unboundedSstIterator{
		sst:         sst,
		f:           f,
		blockBuffer: blockBuffer,
		blockIndex:  0,
		closed:      false,
		nextPair:    nil,
	}, nil
}

// Determines whether a next value exists in the iterator.
// If necessary it will read the next block into an in memory buffer.
func (iter *unboundedSstIterator) Next() (bool, error) {
	if iter.closed {
		return false, common.ERR_ITER_CLOSED
	}

	if iter.blockBuffer.Len() == 0 {
		if iter.blockIndex+1 == len(iter.sst.blocks) {
			return false, nil
		}

		iter.blockIndex++
		iter.f.Seek(iter.sst.blocks[iter.blockIndex].offset, io.SeekStart)
		blockBytes := make([]byte, iter.sst.blocks[iter.blockIndex].usedBytes)
		bytesRead, err := iter.f.Read(blockBytes)
		if err == nil && int64(bytesRead) != iter.sst.blocks[iter.blockIndex].usedBytes {
			err = common.ERR_BLOCK_UNDERFLOW
		}
		if err != nil {
			return false, err
		}
		iter.blockBuffer = bytes.NewReader(blockBytes)
	}

	length := make([]byte, 1)
	_, err := iter.blockBuffer.Read(length)
	if err != nil {
		return false, err
	}
	key := make([]byte, length[0])
	_, err = iter.blockBuffer.Read(key)
	if err != nil {
		return false, err
	}
	_, err = iter.blockBuffer.Read(length)
	if err != nil {
		return false, err
	}
	value := make([]byte, length[0])
	_, err = iter.blockBuffer.Read(value)
	if err != nil {
		return false, err
	}
	iter.nextPair = &common.Pair{Key: key, Value: value}

	return true, nil
}

// Get the current pair in the iterator
func (iter *unboundedSstIterator) Get() (*common.Pair, error) {
	if iter.closed {
		return nil, common.ERR_ITER_CLOSED
	}

	return iter.nextPair, nil
}

// Close the iterator.
// Close is terminal and will cause Next and Get to return errors.
func (iter *unboundedSstIterator) Close() error {
	err := iter.f.Close()
	iter.closed = true
	return err
}
