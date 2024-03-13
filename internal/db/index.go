package db

import "subd/internal/btree"

type Index struct {
	tree *btree.Tree
	name string
}
