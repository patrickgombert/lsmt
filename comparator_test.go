package lsmt

import "testing"

func TestEqual(t *testing.T) {
	testComparison(t, []byte{1, 0, 1}, []byte{1, 0, 1}, EQUAL)
}

func TestGreaterThan(t *testing.T) {
	testComparison(t, []byte{1, 1, 0}, []byte{1, 0, 0}, GREATER_THAN)
}

func TestLessThan(t *testing.T) {
	testComparison(t, []byte{1, 0, 0}, []byte{1, 1, 0}, LESS_THAN)
}

func TestGreaterThanWhenLonger(t *testing.T) {
	testComparison(t, []byte{1, 1, 1, 0}, []byte{1, 1, 1}, GREATER_THAN)
}

func TestLessThanWhenShorter(t *testing.T) {
	testComparison(t, []byte{1, 1, 1}, []byte{1, 1, 1, 0}, LESS_THAN)
}

func testComparison(t *testing.T, a []byte, b []byte, comparison comparison) {
	result := Compare(a, b)
	if result != comparison {
		t.Errorf("Expected %q and %q to compare %q, got %q", a, b, comparison, result)
	}
}
