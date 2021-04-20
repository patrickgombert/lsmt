package common

import (
	"errors"

	c "github.com/patrickgombert/lsmt/comparator"
)

const (
	INIT   int = -1
	CLOSED int = -2
)

type mergedIterator struct {
	iterators       []Iterator
	peek            []*Pair
	next            int
	returnTombstone bool
}

// Creates a new merged iterator from the given slice of iterators.
// The iterators are expected to be passed in priority order, meaning that if two or more
// iterators contain the same key then accept the pair from the iterator at the smaller
// index. Each key will only be returned once from a merged iterator and it is assumed
// that each key only appears once in each provided iterator.
//
// Accepts a returnTombstone parameter which indicates whether to return pairs with a
// tombstone value.
func NewMergedIterator(iterators []Iterator, returnTombstone bool) *mergedIterator {
	peek := make([]*Pair, len(iterators))
	return &mergedIterator{iterators: iterators, peek: peek, next: INIT, returnTombstone: returnTombstone}
}

// Peeks in all iterators and returns true if there exists at least one more pair
// available.
func (iter *mergedIterator) Next() (bool, error) {
	if iter.next == CLOSED {
		return false, nil
	}
	if iter.next == INIT {
		for i := range iter.iterators {
			err := iter.progress(i)
			if err != nil {
				return false, err
			}
		}
	} else {
		err := iter.progress(iter.next)
		if err != nil {
			return false, err
		}
	}

	var minPair *Pair
	for i, next := range iter.peek {
		if next != nil {
			if minPair == nil {
				minPair = next
				iter.next = i
			} else {
				switch c.Compare(next.Key, minPair.Key) {
				case c.LESS_THAN:
					minPair = next
					iter.next = i
				case c.EQUAL:
					err := iter.progress(i)
					if err != nil {
						return false, err
					}
				}
			}
		}
	}

	return minPair != nil, nil
}

// Gets the pair with the least key.
func (iter *mergedIterator) Get() (*Pair, error) {
	if iter.next == INIT {
		return nil, errors.New("Get invoked before Next")
	} else if iter.next == CLOSED {
		return nil, nil
	} else {
		return iter.peek[iter.next], nil
	}
}

// Closes all underlying iterators and returns the latest error from the underlying
// iterators if one occurs.
func (iter *mergedIterator) Close() error {
	var err error
	for i, iterator := range iter.iterators {
		var closeError = iterator.Close()
		if closeError != nil {
			err = closeError
		}
		iter.peek[i] = nil
	}

	iter.next = CLOSED
	return err
}

func (iter *mergedIterator) progress(index int) error {
	next, err := iter.iterators[index].Next()
	if err != nil {
		return err
	}
	if next {
		pair, err := iter.iterators[index].Get()
		if !iter.returnTombstone {
			for c.Compare(pair.Value, Tombstone) == c.EQUAL {
				next, err := iter.iterators[index].Next()
				if err != nil {
					return err
				}

				if next {
					pair, err = iter.iterators[index].Get()
				} else {
					iter.peek[index] = nil
					return nil
				}
			}
		}
		if err != nil {
			return err
		}
		iter.peek[index] = pair
	} else {
		iter.peek[index] = nil
	}
	return nil
}
