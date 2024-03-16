package btree

import (
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
)

type inode []*pair
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
		n.inodes = []*pair{}
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
	*in = append((*in), &pair{})
	copy((*in)[i+1:], (*in)[i:])
	(*in)[i] = &p
}

func (in *inode) removeAt(i int) pair {
	it := (*in)[i]
	copy((*in)[i:], (*in)[i+1:])
	(*in)[len(*in)-1] = &pair{}
	(*in) = (*in)[:len(*in)-1]
	return *it
}

func (in *inode) pop() pair {
	it := (*in)[len(*in)-1]
	(*in)[len(*in)-1] = &pair{}
	(*in) = (*in)[:len(*in)-1]
	return *it
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

func (n *node) getByKey(key KeyType) *pair {
	found, i := n.inodes.search(key)
	if found {
		return n.inodes[i]
	}
	if len(n.children) == 0 {
		// leaf
		return nil
	}
	return n.children[i].getByKey(key)
}

func (n *node) set(key KeyType, value string, maxItem int) {
	found, i := n.inodes.search(key)

	if found {
		n.inodes[i].Value = append(n.inodes[i].Value, value)
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
			n.inodes[i].Value = append(n.inodes[i].Value, value)
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
		tmpVal := n.children[i].removeMax(minItem)
		n.inodes[i] = &tmpVal
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
	child.inodes.insertAt(0, *n.inodes[i-1])
	tmpVal := leftSibling.inodes.pop()
	n.inodes[i-1] = &tmpVal
	if len(child.children) > 0 {
		child.children.insertAt(0, leftSibling.children.pop())
	}
}

func (n *node) extendChildWithRightSibling(i int) {
	child := n.children[i]
	rightSibling := n.children[i+1]
	child.inodes = append(child.inodes, n.inodes[i])
	tmpVal := rightSibling.inodes.removeAt(0)
	n.inodes[i] = &tmpVal
	if len(child.children) > 0 {
		child.children = append(child.children, rightSibling.children.removeAt(0))
	}
}

func (n *node) mergeChildWithRightSibling(i int) {
	child := n.children[i]
	rightSibling := n.children.removeAt(i + 1)
	tmpVal := n.inodes.removeAt(i)
	child.inodes = append(child.inodes, &tmpVal)
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
	return *kv, new
}

func (n *node) removeWithValues(values map[string]interface{}) (emptyItemsKeys []KeyType) {
	emptyItemsKeys = make([]KeyType, 0)
	for _, item := range n.inodes {
		item.Value = slices.DeleteFunc(item.Value, func(val string) bool {
			_, ok := values[val]
			return ok
		})
		if len(item.Value) == 0 {
			emptyItemsKeys = append(emptyItemsKeys, item.Key)
		}
	}

	for _, child := range n.children {
		emptyItemsKeys = append(emptyItemsKeys, child.removeWithValues(values)...)
	}
	return emptyItemsKeys
}
