package btree

func (b *Tree) FindByKey(key string) *Item {
	index, containingNode, _ := b.findKey(key, true)
	if index == -1 {
		return nil
	}
	return containingNode.items[index]
}

func (b *Tree) FindByValue(cmp []Comparator) []*Item {
	return b.root.findValue(cmp)
}

func (b *Tree) findKey(key string, exact bool) (int, *Node, []int) {
	n := b.root

	ancestorsIndexes := []int{0} // index of root
	for {
		wasFound, index := n.findKey(key)
		if wasFound {
			return index, n, ancestorsIndexes
		} else {
			if n.isLeaf() {
				if exact {
					return -1, nil, nil
				}
				return index, n, ancestorsIndexes
			}
			nextChild := n.childNodes[index]
			ancestorsIndexes = append(ancestorsIndexes, index)
			n = nextChild
		}
	}
}
