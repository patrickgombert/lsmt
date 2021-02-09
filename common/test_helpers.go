package common

import (
	"os"
	"testing"

	"github.com/patrickgombert/lsmt/comparator"
	"github.com/patrickgombert/lsmt/config"
)

const TEST_DIR string = "/tmp/lsmt/"

var EMPTY_LEVELS []*config.Level = []*config.Level{}

func CompareGet(iter Iterator, key, value []byte, t *testing.T) {
	pair, _ := iter.Get()
	if comparator.Compare(pair.Key, key) != comparator.EQUAL || comparator.Compare(pair.Value, value) != comparator.EQUAL {
		t.Errorf("Expected Get() to produce %q : %q, but got %q : %q", key, value, pair.Key, pair.Value)
	}
}

func CompareNext(iter Iterator, expected bool, t *testing.T) {
	actual, _ := iter.Next()
	if actual != expected {
		t.Errorf("Expected Next() to produce %t, but got %t.", expected, actual)
	}
}

func SetUp(t *testing.T) {
	if os.Mkdir(TEST_DIR, os.ModeDir|os.ModePerm) != nil {
		t.Errorf("Failed to setUp by creating directory: %s", TEST_DIR)
	}
}

func TearDown(t *testing.T) {
	if os.RemoveAll(TEST_DIR) != nil {
		t.Errorf("Failed to tearDown by removing directory: %s", TEST_DIR)
	}
}
