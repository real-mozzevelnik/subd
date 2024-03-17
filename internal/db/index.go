package db

import "subd/internal/btree"

type index struct {
	tree *btree.Tree
	name string
}

func newIndex(fieldName string) *index {
	return &index{
		name: fieldName,
		tree: btree.New(btree.DefaultBTreeDegree),
	}
}
