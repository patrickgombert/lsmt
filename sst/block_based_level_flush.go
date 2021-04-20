package sst

import (
	"bufio"
	"os"

	"github.com/patrickgombert/lsmt/common"
	"github.com/patrickgombert/lsmt/config"
)

const NOMAX = -1

type blockBasedLevelFlush struct {
	options           *config.Options
	level             config.LevelOptions
	file              *os.File
	writer            *bufio.Writer
	ssts              []*sst
	blocks            []*block
	currentBlock      *block
	previousPair      *common.Pair
	bytesWritten      int64
	currentBlockSize  int64
	totalBytesWritten int64
	maxSize           int64
}

func newFlush(options *config.Options, level config.LevelOptions, maxSize int64) *blockBasedLevelFlush {
	return &blockBasedLevelFlush{
		options:           options,
		level:             level,
		ssts:              []*sst{},
		blocks:            []*block{},
		bytesWritten:      int64(0),
		currentBlockSize:  int64(0),
		totalBytesWritten: int64(0),
		maxSize:           maxSize,
	}
}

// Accept a Pair into the new level.
// Returns true if the pair was accepted, false if the flush is full and it was accepted.
// When the level is full the pair will not have been written.
// The user should call close when the flush is full.
func (flush *blockBasedLevelFlush) accept(pair *common.Pair) (bool, error) {
	// size of the pair plus 2 metadata bytes
	// one byte to hold size of key, one byte to hold size of value
	recordLength := int64(len(pair.Key) + len(pair.Value) + 2)

	if flush.maxSize > NOMAX && flush.totalBytesWritten+recordLength > flush.maxSize {
		return false, nil
	}

	// If the given pair will exceed the file size then close the file and start a new file
	if flush.bytesWritten+recordLength > flush.level.GetSSTSize() {
		flush.ssts[len(flush.ssts)-1].metaOffset = flush.bytesWritten
		flush.currentBlock.end = flush.previousPair.Key
		err := writeMeta(flush.writer, flush.bytesWritten, flush.blocks)
		if err != nil {
			return false, err
		}
		err = flush.file.Close()
		if err != nil {
			return false, err
		}
		flush.file = nil
	}

	// If the file is nil (this is the first pair or the file was just closed) then create
	// a new file
	if flush.file == nil {
		file, err := newFile(flush.options.Path)
		if err != nil {
			return false, err
		}
		flush.file = file
		flush.writer = bufio.NewWriter(flush.file)
		flush.currentBlock = &block{start: pair.Key, offset: 0}
		flush.blocks = []*block{flush.currentBlock}
		flush.ssts = append(flush.ssts, &sst{file: file.Name(), blocks: flush.blocks})
		flush.bytesWritten = int64(0)
		flush.currentBlockSize = int64(0)
	}

	// If the block is going to be exceeded then move to the next block
	if flush.currentBlockSize+recordLength > flush.level.GetBlockSize() {
		flush.currentBlock.end = flush.previousPair.Key
		flush.currentBlock = &block{start: pair.Key, offset: flush.bytesWritten}
		flush.currentBlockSize = int64(0)
		flush.blocks = append(flush.blocks, flush.currentBlock)
		err := flush.writer.Flush()
		if err != nil {
			return false, err
		}
	}

	flush.writer.WriteByte(byte(len(pair.Key)))
	flush.writer.Write(pair.Key)
	flush.writer.WriteByte(byte(len(pair.Value)))
	flush.writer.Write(pair.Value)

	flush.bytesWritten += recordLength
	flush.currentBlockSize += recordLength
	flush.totalBytesWritten += recordLength
	flush.previousPair = pair

	return true, nil
}

// Close out any open SSTs and return all created SSTs
func (flush *blockBasedLevelFlush) close() ([]*sst, error) {
	if len(flush.ssts) > 0 {
		flush.ssts[len(flush.ssts)-1].metaOffset = flush.bytesWritten
		flush.currentBlock.end = flush.previousPair.Key
		err := writeMeta(flush.writer, flush.bytesWritten, flush.blocks)
		if err != nil {
			return nil, err
		}
		err = flush.file.Close()
		if err != nil {
			return nil, err
		}
	}

	return flush.ssts, nil
}

// Write the block metadata to the underlying sst file
func writeMeta(w *bufio.Writer, metaStart int64, blocks []*block) error {
	w.Write(int64toBytes(int64(len(blocks))))
	for _, block := range blocks {
		w.WriteByte(byte(len(block.start)))
		w.Write(block.start)
		w.WriteByte(byte(len(block.end)))
		w.Write(block.end)
		w.Write(int64toBytes(block.offset))
	}
	w.Write(int64toBytes(metaStart))
	return w.Flush()
}
