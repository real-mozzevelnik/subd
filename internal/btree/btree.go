package btree

var DefaultMinItems = 128

type Item struct {
	key   string
	value interface{}
}

type Node struct {
	bucket     *Tree
	items      []*Item
	childNodes []*Node
}

type Tree struct {
	root     *Node
	minItems int
	maxItems int
}

func newItem(key string, value interface{}) *Item {
	return &Item{
		key:   key,
		value: value,
	}
}

func newTreeWithRoot(root *Node, minItems int) *Tree {
	bucket := &Tree{
		root: root,
	}
	bucket.root.bucket = bucket
	bucket.minItems = minItems
	bucket.maxItems = minItems * 2
	return bucket
}

func NewTree(minItems int) *Tree {
	return newTreeWithRoot(NewEmptyNode(), minItems)
}

func (b *Tree) getNodes(indexes []int) []*Node {
	nodes := []*Node{b.root}
	child := b.root
	for i := 1; i < len(indexes); i++ {
		child = child.childNodes[indexes[i]]
		nodes = append(nodes, child)
	}
	return nodes
}

func NewEmptyNode() *Node {
	return &Node{
		items:      []*Item{},
		childNodes: []*Node{},
	}
}

func NewNode(bucket *Tree, value []*Item, childNodes []*Node) *Node {
	return &Node{
		bucket,
		value,
		childNodes,
	}
}

func isLast(index int, parentNode *Node) bool {
	return index == len(parentNode.items)
}

func isFirst(index int) bool {
	return index == 0
}

func (n *Node) isLeaf() bool {
	return len(n.childNodes) == 0
}

func (n *Node) isOverPopulated() bool {
	return len(n.items) > n.bucket.maxItems
}

func (n *Node) isUnderPopulated() bool {
	return len(n.items) < n.bucket.minItems
}

func (n *Node) findKey(key string) (bool, int) {
	for i, existingItem := range n.items {
		if key == existingItem.key {
			return true, i
		}

		if key < existingItem.key {
			return false, i
		}
	}
	return false, len(n.items)
}

func (n *Node) findValue(cmp []Comparator) []*Item {
	items := make([]*Item, 0)
	for _, item := range n.items {

		switch item.value.(type) {
		case []interface{}:
			for _, it := range item.value.([]interface{}) {
				it := newItem(item.key, it)

				isOk := true
				for _, copmarator := range cmp {
					if !copmarator.compare(it) {
						isOk = false
					}
				}
				if isOk {
					items = append(items, it)
				}
			}
		case map[string]interface{}:
			isOk := true
			for _, copmarator := range cmp {
				if !copmarator.compare(item) {
					isOk = false
				}
			}
			if isOk {
				items = append(items, item)
			}
		}

	}
	for _, node := range n.childNodes {
		if node != nil {
			items = append(items, node.findValue(cmp)...)
		}
	}
	return items
}

func (n *Node) addItem(item *Item, insertionIndex int) int {
	if len(n.items) == insertionIndex {
		n.items = append(n.items, item)
		return insertionIndex
	}

	n.items = append(n.items[:insertionIndex+1], n.items[insertionIndex:]...)
	n.items[insertionIndex] = item
	return insertionIndex
}

func (n *Node) addChild(node *Node, insertionIndex int) {
	if len(n.childNodes) == insertionIndex {
		n.childNodes = append(n.childNodes, node)
	}

	n.childNodes = append(n.childNodes[:insertionIndex+1], n.childNodes[insertionIndex:]...)
	n.childNodes[insertionIndex] = node
}

func (n *Node) split(modifiedNode *Node, insertionIndex int) {
	i := 0
	nodeSize := n.bucket.minItems

	for modifiedNode.isOverPopulated() {
		middleItem := modifiedNode.items[nodeSize]
		var newNode *Node
		if modifiedNode.isLeaf() {
			newNode = NewNode(n.bucket, modifiedNode.items[nodeSize+1:], []*Node{})
			modifiedNode.items = modifiedNode.items[:nodeSize]
		} else {
			newNode = NewNode(n.bucket, modifiedNode.items[nodeSize+1:], modifiedNode.childNodes[i+1:])
			modifiedNode.items = modifiedNode.items[:nodeSize]
			modifiedNode.childNodes = modifiedNode.childNodes[:nodeSize+1]
		}
		n.addItem(middleItem, insertionIndex)
		if len(n.childNodes) == insertionIndex+1 {
			n.childNodes = append(n.childNodes, newNode)
		} else {
			n.childNodes = append(n.childNodes[:insertionIndex+1], n.childNodes[insertionIndex:]...)
			n.childNodes[insertionIndex+1] = newNode
		}

		insertionIndex += 1
		i += 1
		modifiedNode = newNode
	}
}

func rotateRight(aNode, pNode, bNode *Node, bNodeIndex int) {
	aNodeItem := aNode.items[len(aNode.items)-1]
	aNode.items = aNode.items[:len(aNode.items)-1]

	pNodeItemIndex := bNodeIndex - 1
	if isFirst(bNodeIndex) {
		pNodeItemIndex = 0
	}
	pNodeItem := pNode.items[pNodeItemIndex]
	pNode.items[pNodeItemIndex] = aNodeItem

	bNode.items = append([]*Item{pNodeItem}, bNode.items...)

	if !aNode.isLeaf() {
		childNodeToShift := aNode.childNodes[len(aNode.childNodes)-1]
		aNode.childNodes = aNode.childNodes[:len(aNode.childNodes)-1]
		bNode.childNodes = append([]*Node{childNodeToShift}, bNode.childNodes...)
	}
}

func rotateLeft(aNode, pNode, bNode *Node, bNodeIndex int) {

	bNodeItem := bNode.items[0]
	bNode.items = bNode.items[1:]

	pNodeItemIndex := bNodeIndex
	if isLast(bNodeIndex, pNode) {
		pNodeItemIndex = len(pNode.items) - 1
	}
	pNodeItem := pNode.items[pNodeItemIndex]
	pNode.items[pNodeItemIndex] = bNodeItem

	aNode.items = append(aNode.items, pNodeItem)

	if !bNode.isLeaf() {
		childNodeToShift := bNode.childNodes[0]
		bNode.childNodes = bNode.childNodes[1:]
		aNode.childNodes = append(aNode.childNodes, childNodeToShift)
	}
}

func merge(pNode *Node, unbalancedNodeIndex int) {
	unbalancedNode := pNode.childNodes[unbalancedNodeIndex]
	if unbalancedNodeIndex == 0 {

		aNode := unbalancedNode
		bNode := pNode.childNodes[unbalancedNodeIndex+1]

		pNodeItem := pNode.items[0]
		pNode.items = pNode.items[1:]
		aNode.items = append(aNode.items, pNodeItem)

		aNode.items = append(aNode.items, bNode.items...)
		pNode.childNodes = append(pNode.childNodes[0:1], pNode.childNodes[2:]...)
		if !bNode.isLeaf() {
			aNode.childNodes = append(aNode.childNodes, bNode.childNodes...)
		}
	} else {

		bNode := unbalancedNode
		aNode := pNode.childNodes[unbalancedNodeIndex-1]

		pNodeItem := pNode.items[unbalancedNodeIndex-1]
		pNode.items = append(pNode.items[:unbalancedNodeIndex-1], pNode.items[unbalancedNodeIndex:]...)
		aNode.items = append(aNode.items, pNodeItem)

		aNode.items = append(aNode.items, bNode.items...)
		pNode.childNodes = append(pNode.childNodes[:unbalancedNodeIndex], pNode.childNodes[unbalancedNodeIndex+1:]...)
		if !aNode.isLeaf() {
			bNode.childNodes = append(aNode.childNodes, bNode.childNodes...)
		}
	}
}
