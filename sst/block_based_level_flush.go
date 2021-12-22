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

// Determines whether the level will accept the pair.
// If false is returned the user must call close without accepting the pair.
func (flush *blockBasedLevelFlush) willAccept(pair *common.Pair) bool {
	additionalBytes := recordLength(pair)
	return flush.maxSize == NOMAX || flush.totalBytesWritten+additionalBytes <= flush.maxSize
}

// Accept a Pair into the new level.
func (flush *blockBasedLevelFlush) accept(pair *common.Pair) error {
	additionalBytes := recordLength(pair)

	// If the given pair will exceed the file size then close the file and start a new file
	if flush.bytesWritten+additionalBytes > flush.level.GetSSTSize() {
		flush.currentBlock.end = flush.previousPair.Key
		flush.currentBlock.usedBytes = flush.currentBlockSize
		remainingBlock := flush.level.GetBlockSize() - flush.currentBlockSize
		if remainingBlock > 0 {
			spacer := make([]byte, remainingBlock)
			flush.writer.Write(spacer)
		}
		flush.bytesWritten += remainingBlock
		flush.totalBytesWritten += remainingBlock
		flush.ssts[len(flush.ssts)-1].metaOffset = flush.bytesWritten

		err := writeMeta(flush.writer, flush.bytesWritten, flush.blocks)
		if err != nil {
			return err
		}
		err = flush.file.Close()
		if err != nil {
			return err
		}
		flush.file = nil
	}

	// If the file is nil (this is the first pair or the file was just closed) then create
	// a new file
	if flush.file == nil {
		file, err := newFile(flush.options.Path)
		if err != nil {
			return err
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
	if flush.currentBlockSize+additionalBytes > flush.level.GetBlockSize() {
		flush.currentBlock.end = flush.previousPair.Key
		flush.currentBlock.usedBytes = flush.currentBlockSize
		remainingBlock := flush.level.GetBlockSize() - flush.currentBlockSize
		if remainingBlock > 0 {
			spacer := make([]byte, remainingBlock)
			flush.writer.Write(spacer)
		}
		flush.bytesWritten += remainingBlock
		flush.totalBytesWritten += remainingBlock
		flush.currentBlock = &block{start: pair.Key, offset: flush.bytesWritten}
		flush.currentBlockSize = int64(0)
		flush.blocks = append(flush.blocks, flush.currentBlock)
		err := flush.writer.Flush()
		if err != nil {
			return err
		}
	}

	flush.writer.WriteByte(byte(len(pair.Key)))
	flush.writer.Write(pair.Key)
	flush.writer.WriteByte(byte(len(pair.Value)))
	flush.writer.Write(pair.Value)

	flush.bytesWritten += additionalBytes
	flush.currentBlockSize += additionalBytes
	flush.totalBytesWritten += additionalBytes
	flush.previousPair = pair

	return nil
}

// Close out any open SSTs and return all created SSTs
func (flush *blockBasedLevelFlush) close() ([]*sst, error) {
	if len(flush.ssts) > 0 {
		remainingBlock := flush.level.GetBlockSize() - flush.currentBlockSize
		if remainingBlock > 0 {
			spacer := make([]byte, remainingBlock)
			flush.writer.Write(spacer)
		}
		flush.bytesWritten += remainingBlock
		flush.ssts[len(flush.ssts)-1].metaOffset = flush.bytesWritten
		flush.currentBlock.end = flush.previousPair.Key
		flush.currentBlock.usedBytes = flush.currentBlockSize
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
		w.Write(int64toBytes(block.usedBytes))
		w.Write(int64toBytes(block.offset))
	}
	w.Write(int64toBytes(metaStart))
	return w.Flush()
}

// Returns the size of the pair plus 2 metadata bytes.
// One byte to hold size of key, one byte to hold size of value
func recordLength(pair *common.Pair) int64 {
	return int64(len(pair.Key) + len(pair.Value) + 2)
}
