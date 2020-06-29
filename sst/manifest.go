package sst

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"strconv"
)

const manifestPrefix string = "manifest"

type Entry struct {
	Path string
}

type Manifest struct {
	Levels [][]Entry
}

func MostRecentManifest(dir string) (*Manifest, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	mostRecent := -1
	manifestFile := ""
	for _, file := range files {
		if file.Name()[:len(manifestPrefix)] == manifestPrefix {
			manifestNumber, err := strconv.Atoi(file.Name()[len(manifestPrefix):])
			if err != nil {
				return nil, err
			}
			if manifestNumber > mostRecent {
				mostRecent = manifestNumber
				manifestFile = file.Name()
			}
		}
	}

	if mostRecent == -1 {
		return nil, nil
	} else {
		return OpenManifest(manifestFile)
	}
}

func OpenManifest(path string) (*Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	intHolder := make([]byte, 4)
	byteHolder := make([]byte, 1)

	_, err = f.Read(intHolder)
	if err != nil {
		return nil, err
	}

	numberOfLevels := binary.BigEndian.Uint32(intHolder)
	entries := make([][]Entry, numberOfLevels)

	for level := 0; level < int(numberOfLevels); level++ {
		_, err = f.Read(intHolder)
		if err != nil {
			return nil, err
		}

		length := binary.BigEndian.Uint32(intHolder)
		entries[level] = make([]Entry, length)

		for i := 0; i < int(length); i++ {
			_, err = f.Read(byteHolder)
			if err != nil {
				return nil, err
			}
			path := make([]byte, int(byteHolder[0]))

			_, err = f.Read(path)
			if err != nil {
				return nil, err
			}

			entries[level][i] = Entry{Path: string(path)}
		}
	}

	return &Manifest{Levels: entries}, nil
}

func WriteManifest(path string, levels [][]SST) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(len(levels)))
	f.Write(b)
	for _, levelSsts := range levels {
		binary.BigEndian.PutUint32(b, uint32(len(levelSsts)))
		f.Write(b)
		for _, sst := range levelSsts {
			f.Write([]byte{byte(len(sst.Path()))})
			f.Write([]byte(sst.Path()))
		}
	}

	return nil
}
