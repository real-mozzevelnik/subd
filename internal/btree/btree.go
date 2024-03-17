package btree

const (
	DefaultBTreeDegree = 128
	freelistSize       = 32
)

type Tree struct {
	root      *node
	degree    int
	maxItem   int
	minItem   int
	nodeCount int
	freeList  *freeList
}

func New(degree int) *Tree {
	return &Tree{
		degree:    degree,
		nodeCount: 0,
		maxItem:   degree*2 - 1,
		minItem:   degree - 1,
		freeList:  newFreeList(freelistSize),
	}
}

func (t *Tree) GetByKey(key KeyType) *pair {
	if t.root == nil {
		return nil
	}
	return t.root.getByKey(key)
}

func (t *Tree) Put(key KeyType, value string) {
	if t.root == nil {
		t.root = t.newNode()
		p := newPair(key, value)
		t.root.inodes.insertAt(0, p)
		return
	}
	if len(t.root.inodes) >= t.maxItem {
		oldRoot := t.root
		t.root = t.newNode()
		index, second := oldRoot.split(t.maxItem / 2)
		t.root.inodes = append(t.root.inodes, &index)
		t.root.children = append(t.root.children, oldRoot)
		t.root.children = append(t.root.children, second)
	}
	t.root.set(key, value, t.maxItem)
}

func (t *Tree) RemoveByKey(key KeyType) (bool, ValueType) {
	if t.root == nil {
		return false, nil
	}
	found, oldValue := t.root.remove(key, t.minItem)
	if len(t.root.inodes) == 0 && len(t.root.children) > 0 {
		emptyroot := t.root
		t.root = t.root.children[0]
		emptyroot.free()
	}
	return found, oldValue
}

func (t *Tree) RemoveWithValues(values []string) {
	if t.root == nil {
		return
	}

	values_map := make(map[string]interface{})
	for _, val := range values {
		values_map[val] = struct{}{}
	}

	emptyItemsKeys := t.root.removeWithValues(values_map)
	for _, emptyItemKey := range emptyItemsKeys {
		t.RemoveByKey(emptyItemKey)
	}

}

func (t *Tree) newNode() *node {
	t.freeList.mu.Lock()
	defer t.freeList.mu.Unlock()
	t.nodeCount++
	if len(t.freeList.list) == 0 {
		return &node{
			id:   t.nodeCount,
			tree: t,
		}
	}
	index := len(t.freeList.list) - 1
	n := t.freeList.list[index]
	t.freeList.list[index] = nil
	t.freeList.list = t.freeList.list[:index]
	return n
}
