package btree

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

type inode []pair
type children []*node

type node struct {
	tree     *Tree
	id       int
	inodes   inode
	children children
}

func (n *node) free() {
	f := n.tree.freeList
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.list) < cap(f.list) {
		n.inodes = []pair{}
		n.children = []*node{}
		f.list = append(f.list, n)
	}
}

func (in inode) search(key KeyType) (bool, int) {
	i := sort.Search(len(in), func(i int) bool {
		return keyLessThan(key, in[i].Key)
	})
	if i > 0 && keyEqualTo(key, in[i-1].Key) {
		return true, i - 1
	}
	return false, i
}

func (in *inode) insertAt(i int, p pair) {
	*in = append((*in), pair{})
	copy((*in)[i+1:], (*in)[i:])
	(*in)[i] = p
}

func (in *inode) removeAt(i int) pair {
	it := (*in)[i]
	copy((*in)[i:], (*in)[i+1:])
	(*in)[len(*in)-1] = pair{}
	(*in) = (*in)[:len(*in)-1]
	return it
}

func (in *inode) pop() pair {
	it := (*in)[len(*in)-1]
	(*in)[len(*in)-1] = pair{}
	(*in) = (*in)[:len(*in)-1]
	return it
}

func (c *children) insertAt(i int, child *node) {
	*c = append(*c, nil)
	copy((*c)[i+1:], (*c)[i:])
	(*c)[i] = child
}

func (c *children) removeAt(i int) *node {
	n := (*c)[i]
	copy((*c)[i:], (*c)[i+1:])
	(*c)[len(*c)-1] = nil
	*c = (*c)[:len(*c)-1]
	return n
}

func (c *children) pop() *node {
	n := (*c)[len(*c)-1]
	(*c)[len(*c)-1] = nil
	(*c) = (*c)[:len(*c)-1]
	return n
}

func (n *node) getByKeyWithOperation(key KeyType, operation string) []*pair {
	items := make([]*pair, 0)

	for _, item := range n.inodes {
		cmp := Comparator{
			Value:     key,
			Operation: operation,
		}
		if cmp.compare(item.Key) {
			switch item.Value.(type) {
			case []interface{}:
				for _, v := range item.Value.([]interface{}) {
					i := newPair(item.Key, v)
					items = append(items, &i)
				}
			default:
				items = append(items, &item)
			}
		}
	}

	for _, child := range n.children {
		if child != nil {
			items = append(items, child.getByKeyWithOperation(key, operation)...)
		}
	}

	return items
}

func (n *node) getByKey(key KeyType) *pair {
	found, i := n.inodes.search(key)
	if found {
		return &n.inodes[i]
	}
	if len(n.children) == 0 {
		// leaf
		return nil
	}
	return n.children[i].getByKey(key)
}

func (n *node) getByValue(cmp []Comparator) []*pair {
	items := make([]*pair, 0)

	for _, item := range n.inodes {
		switch item.Value.(type) {
		case []interface{}:
			for _, it := range item.Value.([]interface{}) {
				it := newPair(item.Key, it)

				isOk := true
				for _, copmarator := range cmp {
					if !copmarator.compare(it.Value.(map[string]interface{})[copmarator.FieldName]) {
						isOk = false
						break
					}
				}
				if isOk {
					items = append(items, &it)
				}
			}
		case map[string]interface{}:
			isOk := true
			for _, copmarator := range cmp {
				if !copmarator.compare(item.Value.(map[string]interface{})[copmarator.FieldName]) {
					isOk = false
					break
				}
			}
			if isOk {
				items = append(items, &item)
			}
		default:
			isOk := true
			for _, copmarator := range cmp {
				if !copmarator.compare(item.Value) {
					isOk = false
					break
				}
			}
			if isOk {
				items = append(items, &item)
			}
		}
	}

	for _, node := range n.children {
		if node != nil {
			items = append(items, node.getByValue(cmp)...)
		}
	}

	return items
}

func (n *node) set(key KeyType, value ValueType, maxItem int) {
	found, i := n.inodes.search(key)

	if found {
		switch n.inodes[i].Value.(type) {
		case []interface{}:
			n.inodes[i].Value = append(n.inodes[i].Value.([]interface{}), value)
		case map[string]interface{}:
			new_val := make([]interface{}, 0)
			new_val = append(new_val, n.inodes[i].Value)
			new_val = append(new_val, value)
			n.inodes[i].Value = new_val
		}
		return
	}

	if len(n.children) == 0 {

		n.inodes.insertAt(i, newPair(key, value))
		return
	}

	child := n.children[i]

	if len(child.inodes) > maxItem {
		newIndex, newChild := child.split(maxItem / 2)

		n.inodes.insertAt(i, newIndex)

		n.children.insertAt(i+1, newChild)
		if keyEqualTo(key, newIndex.Key) {
			n.inodes[i].Value = value
			return
		}
		if keyLessThan(newIndex.Key, key) {
			i++
		}
	}

	n.children[i].set(key, value, maxItem)
}

func (n *node) removeMax(minItem int) pair {
	if len(n.children) == 0 {
		return n.inodes.pop()
	}
	if len(n.children[len(n.children)-1].inodes) <= minItem {
		n.extendChild(len(n.children)-1, minItem)
	}
	return n.children[len(n.children)-1].removeMax(minItem)
}

func (n *node) remove(key KeyType, minItem int) (bool, ValueType) {
	found, i := n.inodes.search(key)
	if len(n.children) == 0 {
		if found {
			return true, n.inodes.removeAt(i).Value
		}
		return false, nil
	}

	if len(n.children[i].inodes) <= minItem {
		n.extendChild(i, minItem)
		found, i = n.inodes.search(key)
	}
	if found {
		removed := n.inodes[i].Value
		n.inodes[i] = n.children[i].removeMax(minItem)
		return true, removed
	}
	return n.children[i].remove(key, minItem)
}

func (n *node) extendChild(i, minItem int) int {
	if i > 0 && len(n.children[i-1].inodes) > minItem {
		n.extendChildWithLeftSibling(i)
		return i
	}
	if i < len(n.children)-1 && len(n.children[i+1].inodes) > minItem {
		n.extendChildWithRightSibling(i)
		return i
	}
	if i == len(n.children)-1 {
		i--
	}
	n.mergeChildWithRightSibling(i)
	return i
}

func (n *node) extendChildWithLeftSibling(i int) {
	child := n.children[i]
	leftSibling := n.children[i-1]
	child.inodes.insertAt(0, n.inodes[i-1])
	n.inodes[i-1] = leftSibling.inodes.pop()
	if len(child.children) > 0 {
		child.children.insertAt(0, leftSibling.children.pop())
	}
}

func (n *node) extendChildWithRightSibling(i int) {
	child := n.children[i]
	rightSibling := n.children[i+1]
	child.inodes = append(child.inodes, n.inodes[i])
	n.inodes[i] = rightSibling.inodes.removeAt(0)
	if len(child.children) > 0 {
		child.children = append(child.children, rightSibling.children.removeAt(0))
	}
}

func (n *node) mergeChildWithRightSibling(i int) {
	child := n.children[i]
	rightSibling := n.children.removeAt(i + 1)
	child.inodes = append(child.inodes, n.inodes.removeAt(i))
	child.inodes = append(child.inodes, rightSibling.inodes...)
	child.children = append(child.children, rightSibling.children...)
	rightSibling.free()
}

func (n *node) print(w io.Writer, level int) {
	fmt.Fprintf(w, "%sNODE:%v\n", strings.Repeat("  ", level), n.inodes)
	for _, c := range n.children {
		c.print(w, level+1)
	}
}

func (n *node) split(i int) (pair, *node) {
	new := n.tree.newNode()
	kv := n.inodes[i]

	new.inodes = append(new.inodes, n.inodes[i+1:]...)
	n.inodes = n.inodes[:i]
	if len(n.children) > 0 {
		new.children = append(new.children, n.children[i+1:]...)
		n.children = n.children[:i+1]
	}
	return kv, new
}
