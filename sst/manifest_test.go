package sst

import (
	"testing"

	"github.com/patrickgombert/lsmt/common"
)

type testSst struct {
	path string
}

func (s *testSst) Path() string {
	return s.path
}

func TestWriteAndReadManifest(t *testing.T) {
	common.SetUp(t)

	levels := make([][]SST, 2)
	levels[0] = []SST{&testSst{path: "./file0.sst"}}
	levels[1] = []SST{&testSst{path: "./file1.sst"}}

	WriteManifest(common.TEST_DIR+"manifest", levels)
	manifest, _ := OpenManifest(common.TEST_DIR + "manifest")

	if len(manifest.Levels) != 2 {
		t.Errorf("Expected %d manifest levels, but got %d", 2, len(manifest.Levels))
	}
	level0 := manifest.Levels[0]
	if len(level0) != 1 {
		t.Errorf("Expected level 0 to have %d entries, but got %d", 1, len(level0))
	}
	if level0[0].Path != "./file0.sst" {
		t.Errorf("Expected level 0 / entry 0 to have file path %q, but got %q", "./file0.sst", level0[0].Path)
	}
	level1 := manifest.Levels[1]
	if len(level1) != 1 {
		t.Errorf("Expected level 1 to have %d entries, but got %d", 1, len(level0))
	}
	if level1[0].Path != "./file1.sst" {
		t.Errorf("Expected level 1 / entry 1 to have file path %q, but got %q", "./file1.sst", level1[0].Path)
	}

	common.TearDown(t)
}
