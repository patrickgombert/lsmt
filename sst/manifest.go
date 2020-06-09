package sst

import (
	"io"
	"os"
)

type Entry struct {
	Path  string
	Level int8
}

type Manifest struct {
	Entries []Entry
}

func OpenManifest(path string) (*Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	entries := []Entry{}
	length := make([]byte, 1)
	for {
		_, err = f.Read(length)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		path := make([]byte, length[0])
		_, err = f.Read(path)
		if err != nil {
			return nil, err
		}

		_, err = f.Read(length)
		if err != nil {
			return nil, err
		}

		entries = append(entries, Entry{Path: string(path), Level: int8(length[0])})
	}

	return &Manifest{Entries: entries}, nil
}

func WriteManifest(path string, ssts [][]SST) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	for level, levelSsts := range ssts {
		levelBytes := []byte{byte(level)}
		for _, sst := range levelSsts {
			f.Write([]byte{byte(len(sst.Path()))})
			f.Write([]byte(sst.Path()))
			f.Write(levelBytes)
		}
	}

	return nil
}
