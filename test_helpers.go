package lsmt

import (
	"os"
	"testing"
)

const TEST_DIR string = "/tmp/lsmt/"

func compareGet(iter iterator, key, value []byte, t *testing.T) {
	pair, _ := iter.Get()
	if Compare(pair.key, key) != EQUAL || Compare(pair.value, value) != EQUAL {
		t.Errorf("Expected Get() to produce %q : %q, but got %q : %q", key, value, pair.key, pair.value)
	}
}

func compareNext(iter iterator, expected bool, t *testing.T) {
	actual, _ := iter.Next()
	if actual != expected {
		t.Errorf("Expected Next() to produce %t, but got %t.", expected, actual)
	}
}

func setUp(t *testing.T) {
	if os.Mkdir(TEST_DIR, os.ModeDir|os.ModePerm) != nil {
		t.Errorf("Failed to setUp by creating directory: %s", TEST_DIR)
	}
}

func tearDown(t *testing.T) {
	if os.RemoveAll(TEST_DIR) != nil {
		t.Errorf("Failed to tearDown by removing directory: %s", TEST_DIR)
	}
}
