package lsmt

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

type block struct {
	start  []byte
	offset uint64
}

type sst struct {
	file   string
	blocks []*block
}

func (sst *sst) Get(key []byte) ([]byte, error) {
	var block *block
	for _, b := range sst.blocks {
		if Compare(key, b.start) == LESS_THAN {
			break
		}
		block = b
	}

	if block == nil {
		return nil, nil
	}

	f, err := os.Open(sst.file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	f.Seek(int64(block.offset), io.SeekStart)
	length := make([]byte, 1)
	for {
		_, err = f.Read(length)
		if err != nil {
			return nil, err
		}
		k := make([]byte, length[0])
		_, err = f.Read(k)
		if err != nil {
			return nil, err
		}

		_, err = f.Read(length)
		if Compare(key, k) == EQUAL {
			v := make([]byte, length[0])
			_, err = f.Read(v)
			if err != nil {
				return nil, err
			}

			return v, nil
		} else {
			_, err := f.Seek(int64(length[0]), io.SeekCurrent)
			if err != nil {
				return nil, err
			}
		}
	}
}

func Flush(options Options, level Level, mt *memtable) ([]*sst, error) {
	if mt == nil {
		return nil, errors.New("unable to flush nil memtable")
	}

	var err error
	var f *os.File
	var w *bufio.Writer

	ssts := []*sst{}
	var blocks []*block
	// TODO implement an unbounded iterator
	iter := mt.Iterator([]byte{}, []byte{255, 255, 255, 255, 255, 255, 255, 255})

	bytesWritten := uint64(0)
	currentBlockSize := uint64(0)
	for iter.Next() {
		k, v := iter.Get()
		recordLength := uint64(len(k) + len(v) + 2)

		if bytesWritten+recordLength > level.sstSize {
			writeMeta(w, bytesWritten, blocks)
			f.Close()
			f = nil
		}

		if f == nil {
			f, err = newFile(options.path)
			if err != nil {
				return nil, err
			}
			w = bufio.NewWriter(f)
			blocks = []*block{&block{start: k, offset: 0}}
			ssts = append(ssts, &sst{file: f.Name(), blocks: blocks})
			bytesWritten = 0
			currentBlockSize = 0
		}

		if currentBlockSize+recordLength > level.blockSize {
			blocks = append(blocks, &block{start: k, offset: bytesWritten})
			w.Flush()
		}

		w.WriteByte(uint8(len(k)))
		w.Write(k)
		w.WriteByte(uint8(len(v)))
		w.Write(v)

		bytesWritten += recordLength
		currentBlockSize += recordLength
	}

	writeMeta(w, bytesWritten, blocks)
	f.Close()

	return ssts, nil
}

func Open(path string) (*sst, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	uint64holder := make([]byte, 8)
	keyLength := make([]byte, 1)

	// Read metadata start relative to start of file
	f.Seek(-8, io.SeekEnd)
	f.Read(uint64holder)
	metaOffset := bytesToUint64(uint64holder)

	// Seek to metadata start
	f.Seek(int64(metaOffset), io.SeekStart)
	// Read number of blocks
	f.Read(uint64holder)
	numBlocks := bytesToUint64(uint64holder)
	blocks := make([]*block, numBlocks)

	for i := uint64(0); i < numBlocks; i++ {
		f.Read(keyLength)
		startKey := make([]byte, keyLength[0])
		f.Read(startKey)
		f.Read(uint64holder)
		block := &block{start: startKey, offset: bytesToUint64(uint64holder)}
		blocks[i] = block
	}

	opened := &sst{file: path, blocks: blocks}
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

func writeMeta(w *bufio.Writer, metaStart uint64, blocks []*block) {
	w.Write(uint64toBytes(uint64(len(blocks))))
	for _, block := range blocks {
		w.WriteByte(uint8(len(block.start)))
		w.Write(block.start)
		w.Write(uint64toBytes(block.offset))
	}
	w.Write(uint64toBytes(metaStart))
	w.Flush()
}

func uint64toBytes(i uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, i)
	return b
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
