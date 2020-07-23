package memtable

import (
	"testing"

	"github.com/patrickgombert/lsmt/common"
)

func TestEmptyIterator(t *testing.T) {
	mt := NewMemtable()

	iter := mt.Iterator([]byte{0}, []byte{1})
	defer iter.Close()

	pair, _ := iter.Get()
	if pair != nil {
		t.Errorf("Expected nil : nil but got %q : %q", pair.Key, pair.Value)
	}
	next, _ := iter.Next()
	if next {
		t.Error("Expected empty iterator to not have Next(), but had Next()")
	}
}

func TestEmptyUnboundedIterator(t *testing.T) {
	mt := NewMemtable()

	iter := mt.UnboundedIterator()
	defer iter.Close()

	pair, _ := iter.Get()
	if pair != nil {
		t.Errorf("Expected nil : nil but got %q : %q", pair.Key, pair.Value)
	}
	next, _ := iter.Next()
	if next {
		t.Error("Expected empty unbounded iterator to not have Next(), but had Next()")
	}
}

func TestIteratorFromStartAndDoesNotHitEndKey(t *testing.T) {
	mt := NewMemtable()
	mt.Write([]byte{1, 1, 1}, []byte{1, 1, 1})
	mt.Write([]byte{0}, []byte{0})
	mt.Write([]byte{1, 1}, []byte{1, 1})
	mt.Write([]byte{0, 1}, []byte{0, 1})

	iter := mt.Iterator([]byte{0, 0}, []byte{1, 1, 1, 1})
	defer iter.Close()

	common.CompareNext(iter, true, t)
	common.CompareGet(iter, []byte{0, 1}, []byte{0, 1}, t)
	common.CompareNext(iter, true, t)
	common.CompareGet(iter, []byte{1, 1}, []byte{1, 1}, t)
	common.CompareNext(iter, true, t)
	common.CompareGet(iter, []byte{1, 1, 1}, []byte{1, 1, 1}, t)
	common.CompareNext(iter, false, t)
}

func TestIteratorFromStartDoesHitEndKey(t *testing.T) {
	mt := NewMemtable()
	mt.Write([]byte{1, 1, 1}, []byte{1, 1, 1})
	mt.Write([]byte{0}, []byte{0})
	mt.Write([]byte{1, 1}, []byte{1, 1})
	mt.Write([]byte{0, 1}, []byte{0, 1})

	iter := mt.Iterator([]byte{}, []byte{1, 1})
	defer iter.Close()

	common.CompareNext(iter, true, t)
	common.CompareGet(iter, []byte{0}, []byte{0}, t)
	common.CompareNext(iter, true, t)
	common.CompareGet(iter, []byte{0, 1}, []byte{0, 1}, t)
	common.CompareNext(iter, true, t)
	common.CompareGet(iter, []byte{1, 1}, []byte{1, 1}, t)
	common.CompareNext(iter, false, t)
}

func TestIteratorPastStart(t *testing.T) {
	mt := NewMemtable()
	mt.Write([]byte{0}, []byte{0})
	mt.Write([]byte{1}, []byte{1})
	mt.Write([]byte{2}, []byte{2})

	iter := mt.Iterator([]byte{1}, []byte{3})
	defer iter.Close()

	common.CompareNext(iter, true, t)
	common.CompareGet(iter, []byte{1}, []byte{1}, t)
	common.CompareNext(iter, true, t)
	common.CompareGet(iter, []byte{2}, []byte{2}, t)
	common.CompareNext(iter, false, t)
}

func TestUnboundedIterator(t *testing.T) {
	mt := NewMemtable()
	mt.Write([]byte{0}, []byte{0})
	mt.Write([]byte{1}, []byte{1})
	mt.Write([]byte{2}, []byte{2})

	iter := mt.UnboundedIterator()
	defer iter.Close()

	common.CompareNext(iter, true, t)
	common.CompareGet(iter, []byte{0}, []byte{0}, t)
	common.CompareNext(iter, true, t)
	common.CompareGet(iter, []byte{1}, []byte{1}, t)
	common.CompareNext(iter, true, t)
	common.CompareGet(iter, []byte{2}, []byte{2}, t)
	common.CompareNext(iter, false, t)
}
