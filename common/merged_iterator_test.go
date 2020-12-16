package common

import (
	"container/list"
	"testing"
)

func TestMergedIteratorsWithNoIterators(t *testing.T) {
	merged := NewMergedIterator([]Iterator{}, true)
	defer merged.Close()

	CompareNext(merged, false, t)
}

func TestMergedIterators(t *testing.T) {
	pairs1 := []*Pair{&Pair{Key: []byte{1}, Value: []byte{1}}, &Pair{Key: []byte{2}, Value: []byte{2}}}
	pairs2 := []*Pair{&Pair{Key: []byte{0}, Value: []byte{0}}, &Pair{Key: []byte{3}, Value: []byte{3}}}
	merged := makeMergedIterator(true, pairs1, pairs2)
	defer merged.Close()

	CompareNext(merged, true, t)
	CompareGet(merged, []byte{0}, []byte{0}, t)
	CompareNext(merged, true, t)
	CompareGet(merged, []byte{1}, []byte{1}, t)
	CompareNext(merged, true, t)
	CompareGet(merged, []byte{2}, []byte{2}, t)
	CompareNext(merged, true, t)
	CompareGet(merged, []byte{3}, []byte{3}, t)
	CompareNext(merged, false, t)
}

func TestMergedIteratorsWithEqualKeys(t *testing.T) {
	pairs1 := []*Pair{&Pair{Key: []byte{0}, Value: []byte{0}}}
	pairs2 := []*Pair{&Pair{Key: []byte{0}, Value: []byte{1}}}
	merged := makeMergedIterator(true, pairs1, pairs2)
	defer merged.Close()

	CompareNext(merged, true, t)
	CompareGet(merged, []byte{0}, []byte{0}, t)
	CompareNext(merged, false, t)
}

func TestMergedIteratorsWithUnevenSize(t *testing.T) {
	pairs1 := []*Pair{&Pair{Key: []byte{1}, Value: []byte{1}}}
	pairs2 := []*Pair{&Pair{Key: []byte{0}, Value: []byte{0}}, &Pair{Key: []byte{2}, Value: []byte{2}}}
	merged := makeMergedIterator(true, pairs1, pairs2)
	defer merged.Close()

	CompareNext(merged, true, t)
	CompareGet(merged, []byte{0}, []byte{0}, t)
	CompareNext(merged, true, t)
	CompareGet(merged, []byte{1}, []byte{1}, t)
	CompareNext(merged, true, t)
	CompareGet(merged, []byte{2}, []byte{2}, t)
	CompareNext(merged, false, t)
}

func TestMergedIteratorCloses(t *testing.T) {
	pairs1 := []*Pair{&Pair{Key: []byte{0}, Value: []byte{0}}}
	pairs2 := []*Pair{&Pair{Key: []byte{0}, Value: []byte{1}}}
	merged := makeMergedIterator(true, pairs1, pairs2)

	CompareNext(merged, true, t)
	CompareGet(merged, []byte{0}, []byte{0}, t)

	merged.Close()

	CompareNext(merged, false, t)
}

func TestMergedIteratorRespectsReturnTombstone(t *testing.T) {
	pairs1 := []*Pair{&Pair{Key: []byte{0}, Value: []byte{0}}}
	pairs2 := []*Pair{&Pair{Key: []byte{1}, Value: Tombstone}}
	merged := makeMergedIterator(true, pairs1, pairs2)

	CompareNext(merged, true, t)
	CompareGet(merged, []byte{0}, []byte{0}, t)
	CompareNext(merged, true, t)
	CompareGet(merged, []byte{1}, Tombstone, t)
	merged.Close()

	merged = makeMergedIterator(false, pairs1, pairs2)
	CompareNext(merged, true, t)
	CompareGet(merged, []byte{0}, []byte{0}, t)
	CompareNext(merged, false, t)
	merged.Close()
}

type listIterator struct {
	next *Pair
	l    *list.List
}

func (li *listIterator) Next() (bool, error) {
	next := li.l.Front()
	if next == nil {
		li.next = nil
		return false, nil
	}

	li.next = next.Value.(*Pair)
	li.l.Remove(next)
	return true, nil
}

func (li *listIterator) Get() (*Pair, error) {
	return li.next, nil
}

func (li *listIterator) Close() error {
	li.l = li.l.Init()
	return nil
}

func makeMergedIterator(returnTombstone bool, pairs ...[]*Pair) *mergedIterator {
	iterators := make([]Iterator, len(pairs))
	for i, p := range pairs {
		l := list.New()
		for _, pair := range p {
			l.PushBack(pair)
		}
		iterators[i] = &listIterator{l: l}
	}
	return NewMergedIterator(iterators, returnTombstone)
}
