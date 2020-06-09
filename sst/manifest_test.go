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

	if len(manifest.Entries) != 2 {
		t.Errorf("Expected %q manifest entries, but got %q", 2, len(manifest.Entries))
	}
	entry0 := manifest.Entries[0]
	if entry0.Path != "./file0.sst" {
		t.Errorf("Expected entry 0 to have file path %q, but got %q", "./file0.sst", entry0.Path)
	}
	if entry0.Level != 0 {
		t.Errorf("Expected entry 0 to have level %d, but got %d", 0, entry0.Level)
	}
	entry1 := manifest.Entries[1]
	if entry1.Path != "./file1.sst" {
		t.Errorf("Expected entry 1 to have file path %q, but got %q", "./file1.sst", entry1.Path)
	}
	if entry1.Level != 1 {
		t.Errorf("Expected entry 1 to have level %d, but got %d", 1, entry1.Level)
	}

	common.TearDown(t)
}
