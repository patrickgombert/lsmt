package memtable

import (
	"math/rand"
	"testing"

	"github.com/patrickgombert/lsmt/common"
	c "github.com/patrickgombert/lsmt/comparator"
)

func TestGetNoKey(t *testing.T) {
	mt := NewMemtable()
	val, found := mt.Get([]byte{0})
	if val != nil {
		t.Error("Expected empty map to not produce a value for Get(), but a value was produced")
	}
	if found {
		t.Error("Expected empty map to not find a value for Get(), but a value was found")
	}
}

func TestGetValue(t *testing.T) {
	mt := NewMemtable()
	key := []byte{1}
	value := []byte{0}
	mt.Write(key, value)
	val, found := mt.Get(key)
	if !found {
		t.Errorf("Expected key %q to be found, but was not found", key)
	}
	if c.Compare(value, val) != c.EQUAL {
		t.Errorf("Expected value %q to equal produced value %q", value, val)
	}
}

func TestInsertAndGetRandomValues(t *testing.T) {
	mt := NewMemtable()
	for i := 0; i < 100; i++ {
		key := randomBytes(1, 100)
		value := randomBytes(0, 100)
		mt.Write(key, value)
		found, _ := mt.Get(key)
		if c.Compare(found, value) != c.EQUAL {
			t.Errorf("Expected value for key %q to equal %q but got %q", key, found, value)
		}
	}
}

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

func randomBytes(minSize, maxSize int) []byte {
	size := rand.Intn(maxSize-minSize) + minSize
	ret := make([]byte, size)
	for i := 0; i < size; i++ {
		ret[i] = byte(rand.Intn(8))
	}
	return ret
}
