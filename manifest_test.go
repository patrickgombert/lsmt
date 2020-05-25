package lsmt

import "testing"

func TestWriteAndReadManifest(t *testing.T) {
	setUp(t)

	levels := make([][]*sst, 2)
	levels[0] = []*sst{&sst{file: "./file0.sst"}}
	levels[1] = []*sst{&sst{file: "./file1.sst"}}

	WriteManifest(TEST_DIR+"manifest", levels)
	manifest, _ := OpenManifest(TEST_DIR + "manifest")

	if len(manifest.entries) != 2 {
		t.Errorf("Expected %q manifest entries, but got %q", 2, len(manifest.entries))
	}
	entry0 := manifest.entries[0]
	if entry0.path != "./file0.sst" {
		t.Errorf("Expected entry 0 to have file path %q, but got %q", "./file0.sst", entry0.path)
	}
	if entry0.level != 0 {
		t.Errorf("Expected entry 0 to have level %d, but got %d", 0, entry0.level)
	}
	entry1 := manifest.entries[1]
	if entry1.path != "./file1.sst" {
		t.Errorf("Expected entry 1 to have file path %q, but got %q", "./file1.sst", entry1.path)
	}
	if entry1.level != 1 {
		t.Errorf("Expected entry 1 to have level %d, but got %d", 1, entry1.level)
	}

	tearDown(t)
}
