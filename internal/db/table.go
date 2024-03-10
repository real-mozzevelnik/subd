package db

import "subd/internal/btree"

type Table struct {
	schema map[string]interface{}
	tree   *btree.Tree
	name   string
}

func newTable(name string, schema map[string]interface{}) *Table {
	return &Table{
		name:   name,
		schema: schema,
		tree:   btree.NewTree(btree.DefaultMinItems),
	}
}

func (t *Table) selectData() {

}

func (t *Table) selectDataWhere() {

}

func (t *Table) insertData() {

}

func (t *Table) deleteData() {

}

func (t *Table) deleteDataWhere() {

}

func (t *Table) updateData() {

}

func (t *Table) updateDataWhere() {

}
