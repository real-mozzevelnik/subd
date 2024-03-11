package btree

import "sync"

type freeList struct {
	mu   *sync.Mutex
	list []*node
}

func newFreeList(size int) *freeList {
	return &freeList{
		mu:   &sync.Mutex{},
		list: make([]*node, 0, size),
	}
}

func (f *freeList) getSize() int {
	return len(f.list)
}
