package memtable

import (
	"github.com/patrickgombert/lsmt/common"
	c "github.com/patrickgombert/lsmt/comparator"
)

// Stack based iterator which keeps the trees lineage in memory while iterating.
type memtableIterator struct {
	init  bool
	stack []persistentNode
	end   []byte
}

// Creates a new bounded iterator for the current state of the memtable.
// Since the memtable is backed by a persistent data structure, this reflects a point in
// time snapshot of the memtable.
func (memtable *Memtable) Iterator(start, end []byte) *memtableIterator {
	stack := make([]persistentNode, 0)
	node := memtable.sortedMap.root
	if node == nil {
		return &memtableIterator{init: false, stack: stack, end: end}
	}
	for c.Compare(start, node.getPair().Key) == c.GREATER_THAN {
		if node.getRight() == nil {
			return &memtableIterator{init: false, stack: stack, end: end}
		}
	}
	stack = appendStack(start, node, stack)
	return &memtableIterator{init: false, stack: stack, end: end}
}

// Moves the iterator forward. Returns false when either the end of the tree has been
// reached or if the end key has been passed.
// The Next() call should never error, but returns a nil error in order to
// satisfy the Iterator interface.
func (iter *memtableIterator) Next() (bool, error) {
	if len(iter.stack) == 0 {
		return false, nil
	}
	if !iter.init {
		iter.init = true
		return true, nil
	}

	idx := len(iter.stack) - 1
	node := iter.stack[idx]
	iter.stack = appendStack(nil, node.getRight(), iter.stack[:idx])
	if len(iter.stack) == 0 {
		return false, nil
	} else {
		idx = len(iter.stack) - 1
		node = iter.stack[idx]
		if c.Compare(node.getPair().Key, iter.end) == c.GREATER_THAN {
			iter.stack = make([]persistentNode, 0)
			return false, nil
		} else {
			return true, nil
		}
	}
}

// Returns the current element's Pair.
// The Get() call should never error, but returns a nil error in order to
// satisfy the Iterator interface.
func (iter *memtableIterator) Get() (*common.Pair, error) {
	if len(iter.stack) == 0 {
		return nil, nil
	}
	pair := iter.stack[len(iter.stack)-1].getPair()
	return &pair, nil
}

// Closes the instance of the iterator which has the effect of making subsequent calls
// to Next() return false and Get() return nil.
func (iter *memtableIterator) Close() error {
	iter.stack = []persistentNode{}
	return nil
}

func appendStack(start []byte, root persistentNode, stack []persistentNode) []persistentNode {
	node := root
	for node != nil {
		if start != nil {
			switch c.Compare(start, node.getPair().Key) {
			case c.EQUAL:
				stack = append(stack, node)
				return stack
			case c.LESS_THAN:
				stack = append(stack, node)
				node = node.getLeft()
			case c.GREATER_THAN:
				node = node.getRight()
			}
		} else {
			stack = append(stack, node)
			node = node.getLeft()
		}
	}
	return stack
}
