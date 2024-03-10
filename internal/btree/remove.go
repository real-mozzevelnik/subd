package btree

func (b *Tree) RemoveByValue(cmp []Comparator) {
	itemsToRemove := b.FindByValue(cmp)
	for _, item := range itemsToRemove {
		b.RemoveByKey(item.key)
	}
}

func (b *Tree) RemoveByKey(key string) {

	removeItemIndex, nodeToRemoveFrom, ancestorsIndexes := b.findKey(key, true)

	if nodeToRemoveFrom.isLeaf() {
		nodeToRemoveFrom.removeItemFromLeaf(removeItemIndex)
	} else {
		affectedNodes := nodeToRemoveFrom.removeItemFromInternal(removeItemIndex)
		ancestorsIndexes = append(ancestorsIndexes, affectedNodes...)
	}

	ancestors := b.getNodes(ancestorsIndexes)

	for i := len(ancestors) - 2; i >= 0; i-- {
		pnode := ancestors[i]
		node := ancestors[i+1]
		if node.isUnderPopulated() {
			pnode.rebalanceRemove(ancestorsIndexes[i+1])
		}
	}

	if len(b.root.items) == 0 && len(b.root.childNodes) > 0 {
		b.root = ancestors[1]
	}
}

func (n *Node) removeItemFromLeaf(index int) {
	n.items = append(n.items[:index], n.items[index+1:]...)
}

func (n *Node) removeItemFromInternal(index int) []int {

	affectedNodes := make([]int, 0)
	affectedNodes = append(affectedNodes, index)

	aNode := n.childNodes[index]
	for !aNode.isLeaf() {
		traversingIndex := len(n.childNodes) - 1
		aNode = n.childNodes[traversingIndex]
		affectedNodes = append(affectedNodes, traversingIndex)
	}

	n.items[index] = aNode.items[len(aNode.items)-1]
	aNode.items = aNode.items[:len(aNode.items)-1]
	return affectedNodes
}

func (n *Node) rebalanceRemove(unbalancedNodeIndex int) {
	pNode := n
	unbalancedNode := pNode.childNodes[unbalancedNodeIndex]

	// right rotate
	var leftNode *Node
	if unbalancedNodeIndex != 0 {
		leftNode = pNode.childNodes[unbalancedNodeIndex-1]
		if len(leftNode.items) > n.bucket.minItems {
			rotateRight(leftNode, pNode, unbalancedNode, unbalancedNodeIndex)
			return
		}
	}

	// left Balance
	var rightNode *Node
	if unbalancedNodeIndex != len(pNode.childNodes)-1 {
		rightNode = pNode.childNodes[unbalancedNodeIndex+1]
		if len(rightNode.items) > n.bucket.minItems {
			rotateLeft(unbalancedNode, pNode, rightNode, unbalancedNodeIndex)
			return
		}
	}

	merge(pNode, unbalancedNodeIndex)
}
