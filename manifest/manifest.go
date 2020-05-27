package manifest

import (
	"io"
	"os"

	"github.com/patrickgombert/lsmt/sst"
)

type entry struct {
	path  string
	level int8
}

type manifest struct {
	entries []entry
}

func OpenManifest(path string) (*manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	entries := []entry{}
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

		entries = append(entries, entry{path: string(path), level: int8(length[0])})
	}

	return &manifest{entries: entries}, nil
}

func WriteManifest(path string, ssts [][]sst.SST) error {
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
