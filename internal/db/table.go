package db

import (
	"subd/internal/btree"

	"github.com/google/uuid"
)

type Table struct {
	schema map[string]interface{}
	tree   *btree.Tree
	name   string
}

func newTable(name string, schema map[string]interface{}) *Table {
	return &Table{
		name:   name,
		schema: schema,
		tree:   btree.New(128),
	}
}

func (t *Table) selectData() []map[string]interface{} {
	rows := make([]map[string]interface{}, 0)
	items := t.tree.GetByValue([]btree.Comparator{})
	for _, item := range items {
		rows = append(rows, item.Value.(map[string]interface{}))
	}
	return rows
}

func (t *Table) selectDataWhere(cmp []Comparator) []map[string]interface{} {
	comparators := make([]btree.Comparator, 0)
	for _, c := range cmp {
		comparators = append(comparators, c.toBTreeComparator())
	}

	rows := make([]map[string]interface{}, 0)
	items := t.tree.GetByValue(comparators)
	for _, item := range items {
		rows = append(rows, item.Value.(map[string]interface{}))
	}
	return rows
}

func (t *Table) insertData(data map[string]interface{}) {
	rowId := uuid.New().String()
	t.tree.Put(btree.KeyType(rowId), data)
}

func (t *Table) deleteData() {

}

func (t *Table) deleteDataWhere() {

}

func (t *Table) updateData() {

}

func (t *Table) updateDataWhere() {

}
