package common

import "testing"

func TestEmptyIterator(t *testing.T) {
	iter := EmptyIterator()
	CompareNext(iter, false, t)
	get, _ := iter.Get()
	if get != nil {
		t.Errorf("Expected empty iterator Get to produce nil but got %v", get)
	}
}
