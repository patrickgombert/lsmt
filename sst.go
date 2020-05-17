package lsmt

import (
	"bufio"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
)

type block struct {
	start  []byte
	offset uint64
}

type sst struct {
	file   string
	blocks []block
}

func Flush(options Options, level Level, mt *memtable) ([]sst, error) {
	if mt == nil {
		return nil, errors.New("unable to flush nil memtable")
	}

	f, err := newFile(options.path)
	w := bufio.NewWriter(f)

	numSstFiles := mt.sortedMap.bytes / level.sstSize
	if numSstFiles == 0 {
		numSstFiles = 1
	}
	ssts := make([]sst, numSstFiles)
	sstsIdx := 0
	blocks := make([]block, level.sstSize/level.blockSize)
	blocksIdx := 0
	ssts[sstsIdx] = sst{file: f.Name(), blocks: blocks}
	// TODO implement an unbounded iterator
	iter := mt.Iterator([]byte{}, []byte{255, 255, 255, 255, 255, 255, 255, 255})

	bytesWritten := uint64(0)
	currentBlockSize := uint64(0)
	for iter.Next() {
		k, v := iter.Get()
		recordLength := uint64(len(k) + len(v) + 2)

		if bytesWritten+recordLength > level.sstSize {
			w.Flush()
			f.Close()

			f, err = newFile(options.path)
			if err != nil {
				return nil, err
			}
			w = bufio.NewWriter(f)
			blocks = make([]block, level.sstSize/level.blockSize)
			blocksIdx = 0
			sstsIdx += 1
			ssts[sstsIdx] = sst{file: f.Name(), blocks: blocks}
			bytesWritten = 0
			currentBlockSize = 0
		}

		if currentBlockSize+recordLength > level.blockSize {
			blocks[blocksIdx] = block{start: k, offset: bytesWritten}
			blocksIdx += 1
		}

		w.Write([]byte{uint8(len(k))})
		w.Write(k)
		w.Write([]byte{uint8(len(v))})
		w.Write(v)

		bytesWritten += recordLength
		currentBlockSize += recordLength
	}
	w.Flush()
	f.Close()

	return ssts, nil
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
