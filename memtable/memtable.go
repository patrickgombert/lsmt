package memtable

import (
	"github.com/patrickgombert/lsmt/common"
	c "github.com/patrickgombert/lsmt/comparator"
)

// Used to denote a red or black node
type color int8

const (
	RED   color = 0
	BLACK color = 1
)

type persistentNode interface {
	getColor() color
	getPair() common.Pair
	getLeft() persistentNode
	getRight() persistentNode
	addLeft(left persistentNode) persistentNode
	addRight(right persistentNode) persistentNode
	balanceLeft(other persistentNode) persistentNode
	balanceRight(other persistentNode) persistentNode
	blacken() persistentNode
	redden() persistentNode
	replace(pair common.Pair, left, right persistentNode) persistentNode
}

type blackNode struct {
	pair common.Pair
}

type blackBranch struct {
	pair  common.Pair
	left  persistentNode
	right persistentNode
}

type redNode struct {
	pair common.Pair
}

type redBranch struct {
	pair  common.Pair
	left  persistentNode
	right persistentNode
}

type persistentSortedMap struct {
	root  persistentNode
	count int64
	bytes int64
}

type Memtable struct {
	sortedMap *persistentSortedMap
}

func (node *blackNode) getColor() color {
	return BLACK
}

func (node *blackNode) getPair() common.Pair {
	return node.pair
}

func (node *blackNode) getLeft() persistentNode {
	return nil
}

func (node *blackNode) getRight() persistentNode {
	return nil
}

func (node *blackNode) addLeft(left persistentNode) persistentNode {
	return left.balanceLeft(node)
}

func (node *blackNode) addRight(right persistentNode) persistentNode {
	return right.balanceRight(node)
}

func (node *blackNode) balanceLeft(other persistentNode) persistentNode {
	return makeBlackNode(other.getPair(), node, other.getRight())
}

func (node *blackNode) balanceRight(other persistentNode) persistentNode {
	return makeBlackNode(other.getPair(), other.getLeft(), node)
}

func (node *blackNode) blacken() persistentNode {
	return node
}

func (node *blackNode) redden() persistentNode {
	return &redNode{pair: node.pair}
}

func (node *blackNode) replace(pair common.Pair, left, right persistentNode) persistentNode {
	return makeBlackNode(pair, left, right)
}

func (node *blackBranch) getColor() color {
	return BLACK
}

func (node *blackBranch) getPair() common.Pair {
	return node.pair
}

func (node *blackBranch) getLeft() persistentNode {
	return node.left
}

func (node *blackBranch) getRight() persistentNode {
	return node.right
}

func (node *blackBranch) addLeft(left persistentNode) persistentNode {
	return left.balanceLeft(node)
}

func (node *blackBranch) addRight(right persistentNode) persistentNode {
	return right.balanceRight(node)
}

func (node *blackBranch) balanceLeft(other persistentNode) persistentNode {
	return makeBlackNode(other.getPair(), node, other.getRight())
}

func (node *blackBranch) balanceRight(other persistentNode) persistentNode {
	return makeBlackNode(other.getPair(), other.getLeft(), node)
}

func (node *blackBranch) blacken() persistentNode {
	return node
}

func (node *blackBranch) redden() persistentNode {
	return &redBranch{pair: node.pair, left: node.left, right: node.right}
}

func (node *blackBranch) replace(pair common.Pair, left, right persistentNode) persistentNode {
	return makeBlackNode(pair, left, right)
}

func (node *redNode) getColor() color {
	return RED
}

func (node *redNode) getPair() common.Pair {
	return node.pair
}

func (node *redNode) getLeft() persistentNode {
	return nil
}

func (node *redNode) getRight() persistentNode {
	return nil
}

func (node *redNode) addLeft(left persistentNode) persistentNode {
	return makeRedNode(node.pair, left, nil)
}

func (node *redNode) addRight(right persistentNode) persistentNode {
	return makeRedNode(node.pair, nil, right)
}

func (node *redNode) balanceLeft(other persistentNode) persistentNode {
	return makeBlackNode(other.getPair(), node, other.getRight())
}

func (node *redNode) balanceRight(other persistentNode) persistentNode {
	return makeBlackNode(other.getPair(), other.getLeft(), node)
}

func (node *redNode) blacken() persistentNode {
	return &blackNode{pair: node.pair}
}

func (node *redNode) redden() persistentNode {
	panic("can not redden redNode")
}

func (node *redNode) replace(pair common.Pair, left, right persistentNode) persistentNode {
	return makeRedNode(pair, left, right)
}

func (node *redBranch) getColor() color {
	return RED
}

func (node *redBranch) getPair() common.Pair {
	return node.pair
}

func (node *redBranch) getLeft() persistentNode {
	return node.left
}

func (node *redBranch) getRight() persistentNode {
	return node.right
}

func (node *redBranch) addLeft(left persistentNode) persistentNode {
	return makeRedNode(node.pair, left, node.right)
}

func (node *redBranch) addRight(right persistentNode) persistentNode {
	return makeRedNode(node.pair, node.left, right)
}

func (node *redBranch) balanceLeft(other persistentNode) persistentNode {
	if node.left != nil && node.left.getColor() == RED {
		blackenedNode := makeBlackNode(other.getPair(), node.right, other.getRight())
		return makeRedNode(node.pair, node.left.blacken(), blackenedNode)
	} else if node.right != nil && node.right.getColor() == RED {
		blackenedLeftNode := makeBlackNode(node.pair, node.left, node.right.getLeft())
		blackenedRightNode := makeBlackNode(other.getPair(), node.right.getRight(), other.getRight())
		pair := common.Pair{Key: node.right.getPair().Key, Value: node.right.getPair().Value}
		return makeRedNode(pair, blackenedLeftNode, blackenedRightNode)
	} else {
		return makeBlackNode(other.getPair(), node, other.getRight())
	}
}

func (node *redBranch) balanceRight(other persistentNode) persistentNode {
	if node.right != nil && node.right.getColor() == RED {
		blackenedNode := makeBlackNode(other.getPair(), other.getLeft(), node.left)
		return makeRedNode(node.pair, blackenedNode, node.right.blacken())
	} else if node.left != nil && node.left.getColor() == RED {
		blackenedLeftNode := makeBlackNode(other.getPair(), other.getLeft(), node.left.getLeft())
		blackenedRightNode := makeBlackNode(node.pair, node.left.getRight(), node.right)
		pair := common.Pair{Key: node.left.getPair().Key, Value: node.left.getPair().Value}
		return makeRedNode(pair, blackenedLeftNode, blackenedRightNode)
	} else {
		return makeBlackNode(other.getPair(), other.getLeft(), node)
	}
}

func (node *redBranch) blacken() persistentNode {
	return &blackBranch{node.pair, node.left, node.right}
}

func (node *redBranch) redden() persistentNode {
	panic("can not redden redBranch")
}

func (node *redBranch) replace(pair common.Pair, left, right persistentNode) persistentNode {
	return makeRedNode(pair, left, right)
}

func makeBlackNode(pair common.Pair, left, right persistentNode) persistentNode {
	if left == nil && right == nil {
		return &blackNode{pair: pair}
	} else {
		return &blackBranch{pair: pair, left: left, right: right}
	}
}

func makeRedNode(pair common.Pair, left, right persistentNode) persistentNode {
	if left == nil && right == nil {
		return &redNode{pair: pair}
	} else {
		return &redBranch{pair: pair, left: left, right: right}
	}
}

// Creates a new instance of a Memtable
func NewMemtable() *Memtable {
	sortedMap := &persistentSortedMap{root: nil, count: 0, bytes: 0}
	return &Memtable{sortedMap: sortedMap}
}

// Returns the value for a given key. The second return value signals whether the key was
// found or not found.
func (memtable *Memtable) Get(key []byte) ([]byte, bool) {
	node := memtable.sortedMap.root
	for {
		if node == nil {
			return nil, false
		}
		comparison := c.Compare(key, node.getPair().Key)
		switch comparison {
		case c.EQUAL:
			if c.Compare(node.getPair().Value, common.Tombstone) == c.EQUAL {
				return nil, false
			} else {
				return node.getPair().Value, true
			}
		case c.LESS_THAN:
			node = node.getLeft()
		case c.GREATER_THAN:
			node = node.getRight()
		}
	}
}

func (memtable *Memtable) Write(key, value []byte) {
	sortedMap := memtable.sortedMap
	if sortedMap.root == nil {
		pair := common.Pair{Key: key, Value: value}
		root := &redNode{pair: pair}
		bytes := leni64(key) + leni64(value)
		memtable.sortedMap = &persistentSortedMap{root: root, count: 1, bytes: bytes}
	} else {
		node, existed := addNode(sortedMap.root, key, value)
		if existed && c.Compare(value, node.getPair().Value) != c.EQUAL {
			bytes := (sortedMap.bytes - leni64(node.getPair().Value)) + leni64(value)
			root := replaceNode(sortedMap.root, key, value)
			memtable.sortedMap = &persistentSortedMap{root: root, count: sortedMap.count, bytes: bytes}
		} else {
			blackenedNode := node.blacken()
			count := sortedMap.count + 1
			bytes := sortedMap.bytes + leni64(key) + leni64(value)
			memtable.sortedMap = &persistentSortedMap{root: blackenedNode, count: count, bytes: bytes}
		}
	}
}

func addNode(root persistentNode, key, value []byte) (persistentNode, bool) {
	if root == nil {
		pair := common.Pair{Key: key, Value: value}
		return &redNode{pair: pair}, false
	}

	comparison := c.Compare(key, root.getPair().Key)
	if comparison == c.EQUAL {
		return root, true
	} else {
		var node persistentNode
		var existed bool
		if comparison == c.LESS_THAN {
			node, existed = addNode(root.getLeft(), key, value)
		} else {
			node, existed = addNode(root.getRight(), key, value)
		}
		if existed {
			return node, true
		} else {
			if comparison == c.LESS_THAN {
				return root.addLeft(node), false
			} else {
				return root.addRight(node), false
			}
		}
	}
}

func replaceNode(root persistentNode, key, value []byte) persistentNode {
	comparison := c.Compare(key, root.getPair().Key)
	var newValue []byte
	var left persistentNode
	var right persistentNode
	switch comparison {
	case c.EQUAL:
		newValue = value
		left = root.getLeft()
		right = root.getRight()
	case c.LESS_THAN:
		newValue = root.getPair().Value
		left = replaceNode(root.getLeft(), key, value)
		right = root.getRight()
	case c.GREATER_THAN:
		left = root.getLeft()
		right = replaceNode(root.getRight(), key, value)
		newValue = root.getPair().Value
	}

	pair := common.Pair{Key: key, Value: newValue}
	return root.replace(pair, left, right)
}

func leni64(bytes []byte) int64 {
	return int64(len(bytes))
}
