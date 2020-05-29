package memtable

import (
	"github.com/patrickgombert/lsmt/common"
	c "github.com/patrickgombert/lsmt/comparator"
)

type memtableIterator struct {
	init  bool
	stack []persistentNode
	end   []byte
}

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

func (iter *memtableIterator) Get() (*common.Pair, error) {
	if len(iter.stack) == 0 {
		return nil, nil
	}
	pair := iter.stack[len(iter.stack)-1].getPair()
	return &pair, nil
}

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
