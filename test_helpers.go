package lsmt

import "testing"

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
