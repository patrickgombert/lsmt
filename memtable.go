package lsmt

type color int8

const (
	RED   color = 0
	BLACK color = 1
)

type pair struct {
	key   []byte
	value []byte
}

type persistentNode interface {
	getColor() color
	getPair() pair
	getLeft() persistentNode
	getRight() persistentNode
	addLeft(left persistentNode) persistentNode
	addRight(right persistentNode) persistentNode
	balanceLeft(other persistentNode) persistentNode
	balanceRight(other persistentNode) persistentNode
	blacken() persistentNode
	redden() persistentNode
	replace(pair pair, left, right persistentNode) persistentNode
}

type blackNode struct {
	pair pair
}

type blackBranch struct {
	pair  pair
	left  persistentNode
	right persistentNode
}

type redNode struct {
	pair pair
}

type redBranch struct {
	pair  pair
	left  persistentNode
	right persistentNode
}

type persistentSortedMap struct {
	root  persistentNode
	count uint64
	bytes uint64
}

type memtable struct {
	sortedMap *persistentSortedMap
}

type iterator struct {
	init  bool
	stack []persistentNode
	end   []byte
}

func (node *blackNode) getColor() color {
	return BLACK
}

func (node *blackNode) getPair() pair {
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

func (node *blackNode) replace(pair pair, left, right persistentNode) persistentNode {
	return makeBlackNode(pair, left, right)
}

func (node *blackBranch) getColor() color {
	return BLACK
}

func (node *blackBranch) getPair() pair {
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

func (node *blackBranch) replace(pair pair, left, right persistentNode) persistentNode {
	return makeBlackNode(pair, left, right)
}

func (node *redNode) getColor() color {
	return RED
}

func (node *redNode) getPair() pair {
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

func (node *redNode) replace(pair pair, left, right persistentNode) persistentNode {
	return makeRedNode(pair, left, right)
}

func (node *redBranch) getColor() color {
	return RED
}

func (node *redBranch) getPair() pair {
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
		pair := pair{key: node.right.getPair().key, value: node.right.getPair().value}
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
		pair := pair{key: node.left.getPair().key, value: node.left.getPair().value}
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

func (node *redBranch) replace(pair pair, left, right persistentNode) persistentNode {
	return makeRedNode(pair, left, right)
}

func makeBlackNode(pair pair, left, right persistentNode) persistentNode {
	if left == nil && right == nil {
		return &blackNode{pair: pair}
	} else {
		return &blackBranch{pair: pair, left: left, right: right}
	}
}

func makeRedNode(pair pair, left, right persistentNode) persistentNode {
	if left == nil && right == nil {
		return &redNode{pair: pair}
	} else {
		return &redBranch{pair: pair, left: left, right: right}
	}
}

func Memtable() *memtable {
	sortedMap := &persistentSortedMap{root: nil, count: 0, bytes: 0}
	return &memtable{sortedMap: sortedMap}
}

func (memtable *memtable) Get(key []byte) ([]byte, bool) {
	node := memtable.sortedMap.root
	for {
		if node == nil {
			return nil, false
		}
		comparison := Compare(key, node.getPair().key)
		switch comparison {
		case EQUAL:
			return node.getPair().value, true
		case LESS_THAN:
			node = node.getLeft()
		case GREATER_THAN:
			node = node.getRight()
		}
	}
}

func (memtable *memtable) Write(key, value []byte) {
	sortedMap := memtable.sortedMap
	if sortedMap.root == nil {
		pair := pair{key: key, value: value}
		root := &redNode{pair: pair}
		bytes := lenu64(key) + lenu64(value)
		memtable.sortedMap = &persistentSortedMap{root: root, count: 1, bytes: bytes}
	} else {
		node, existed := addNode(sortedMap.root, key, value)
		if existed && Compare(value, node.getPair().value) != EQUAL {
			bytes := (sortedMap.bytes - lenu64(node.getPair().value)) + lenu64(value)
			root := replaceNode(sortedMap.root, key, value)
			memtable.sortedMap = &persistentSortedMap{root: root, count: sortedMap.count, bytes: bytes}
		} else {
			blackenedNode := node.blacken()
			count := sortedMap.count + 1
			bytes := sortedMap.bytes + lenu64(key) + lenu64(value)
			memtable.sortedMap = &persistentSortedMap{root: blackenedNode, count: count, bytes: bytes}
		}
	}
}

func (memtable *memtable) Iterator(start, end []byte) *iterator {
	stack := make([]persistentNode, 0)
	node := memtable.sortedMap.root
	if node == nil {
		return &iterator{init: false, stack: stack, end: end}
	}
	for Compare(start, node.getPair().key) == GREATER_THAN {
		if node.getRight() == nil {
			return &iterator{init: false, stack: stack, end: end}
		}
	}
	stack = appendStack(start, node, stack)
	return &iterator{init: false, stack: stack, end: end}
}

func (iter *iterator) Next() bool {
	if len(iter.stack) == 0 {
		return false
	}
	if !iter.init {
		iter.init = true
		return true
	}

	idx := len(iter.stack) - 1
	node := iter.stack[idx]
	iter.stack = appendStack(nil, node.getRight(), iter.stack[:idx])
	if len(iter.stack) == 0 {
		return false
	} else {
		idx = len(iter.stack) - 1
		node = iter.stack[idx]
		if Compare(node.getPair().key, iter.end) == GREATER_THAN {
			iter.stack = make([]persistentNode, 0)
			return false
		} else {
			return true
		}
	}
}

func (iter *iterator) Get() ([]byte, []byte) {
	if len(iter.stack) == 0 {
		return nil, nil
	}
	pair := iter.stack[len(iter.stack)-1].getPair()
	return pair.key, pair.value
}

func addNode(root persistentNode, key, value []byte) (persistentNode, bool) {
	if root == nil {
		pair := pair{key: key, value: value}
		return &redNode{pair: pair}, false
	}

	comparison := Compare(key, root.getPair().key)
	if comparison == EQUAL {
		return root, true
	} else {
		var node persistentNode
		var existed bool
		if comparison == LESS_THAN {
			node, existed = addNode(root.getLeft(), key, value)
		} else {
			node, existed = addNode(root.getRight(), key, value)
		}
		if existed {
			return node, true
		} else {
			if comparison == LESS_THAN {
				return root.addLeft(node), false
			} else {
				return root.addRight(node), false
			}
		}
	}
}

func replaceNode(root persistentNode, key, value []byte) persistentNode {
	comparison := Compare(key, root.getPair().key)
	var newValue []byte
	var left persistentNode
	var right persistentNode
	switch comparison {
	case EQUAL:
		newValue = value
		left = root.getLeft()
		right = root.getRight()
	case LESS_THAN:
		newValue = root.getPair().value
		left = replaceNode(root.getLeft(), key, value)
		right = root.getRight()
	case GREATER_THAN:
		left = root.getLeft()
		right = replaceNode(root.getRight(), key, value)
		newValue = root.getPair().value
	}

	pair := pair{key: key, value: newValue}
	return root.replace(pair, left, right)
}

func appendStack(start []byte, root persistentNode, stack []persistentNode) []persistentNode {
	node := root
	for node != nil {
		if start != nil {
			switch Compare(start, node.getPair().key) {
			case EQUAL:
				stack = append(stack, node)
				return stack
			case LESS_THAN:
				stack = append(stack, node)
				node = node.getLeft()
			case GREATER_THAN:
				node = node.getRight()
			}
		} else {
			stack = append(stack, node)
			node = node.getLeft()
		}
	}
	return stack
}

func lenu64(bytes []byte) uint64 {
	return uint64(len(bytes))
}
