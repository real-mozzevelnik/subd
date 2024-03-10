package btree

func (b *Tree) Put(key string, value map[string]interface{}) {

	// check if value exists - save as slice
	index, node, _ := b.findKey(key, true)
	if index != -1 {
		slice_val := make([]interface{}, 0)
		switch node.items[index].value.(type) {
		case []interface{}:
			slice_val = append(slice_val, node.items[index].value.([]interface{})...)
		case map[string]interface{}:
			slice_val = append(slice_val, node.items[index].value)
		}
		slice_val = append(slice_val, value)
		node.items[index].value = slice_val
		return
	}

	i := newItem(key, value)
	insertionIndex, nodeToInsertIn, ancestorsIndexes := b.findKey(i.key, false)

	nodeToInsertIn.addItem(i, insertionIndex)

	ancestors := b.getNodes(ancestorsIndexes)

	for i := len(ancestors) - 2; i >= 0; i-- {
		pnode := ancestors[i]
		node := ancestors[i+1]
		nodeIndex := ancestorsIndexes[i+1]
		if node.isOverPopulated() {
			pnode.split(node, nodeIndex)
		}
	}

	if b.root.isOverPopulated() {
		newRoot := NewNode(b, []*Item{}, []*Node{b.root})
		newRoot.split(b.root, 0)
		b.root = newRoot
	}
}
