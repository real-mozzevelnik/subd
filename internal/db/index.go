package db

import "subd/internal/btree"

type Index struct {
	tree *btree.Tree
	name string
}

func newIndex(fieldName string) *Index {
	return &Index{
		name: fieldName,
		tree: btree.New(btree.DefaultBTreeDegree),
	}
}
