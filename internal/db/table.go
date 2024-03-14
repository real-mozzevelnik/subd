package db

import (
	"subd/internal/btree"

	"github.com/google/uuid"
)

type Table struct {
	schema  map[string]interface{}
	indexes map[string]*Index
	tree    *btree.Tree
	name    string
}

func newTable(name string, schema map[string]interface{}) *Table {
	return &Table{
		name:    name,
		schema:  schema,
		tree:    btree.New(btree.DefaultBTreeDegree),
		indexes: make(map[string]*Index),
	}
}

func (t *Table) drop() {
	for fieldName, _ := range t.indexes {
		t.dropIndex(fieldName)
	}
	t.deleteData()
}

func (t *Table) newIndex(fieldName string) {
	t.indexes[fieldName] = newIndex(fieldName)
	tableData := t.selectData()
	for _, row := range tableData {
		t.indexes[fieldName].tree.Put(row.Value.(map[string]interface{})[fieldName], row.Key)
	}
}

func (t *Table) dropIndex(fieldName string) {
	t.indexes[fieldName].tree.RemoveByValue([]btree.Comparator{})
	delete(t.indexes, fieldName)
}

func (t *Table) selectData() []*Result {
	rows := make([]*Result, 0)
	items := t.tree.GetByValue([]btree.Comparator{})
	for _, item := range items {
		result := newResult(item.Key, item.Value)
		rows = append(rows, result)
	}
	return rows
}

func (t *Table) selectDataWhere(cmp []Comparator) []*Result {
	comparators := make([]btree.Comparator, 0)
	for _, c := range cmp {
		comparators = append(comparators, c.toBTreeComparator())
	}

	rows := make([]*Result, 0)
	items := t.tree.GetByValue(comparators)
	for _, item := range items {
		result := newResult(item.Key, item.Value)
		rows = append(rows, result)
	}
	return rows
}

func (t *Table) insertData(data map[string]interface{}) {
	rowId := uuid.NewString()
	t.tree.Put(btree.KeyType(rowId), data)

	for fieldName, index := range t.indexes {
		index.tree.Put(data[fieldName], rowId)
	}
}

func (t *Table) deleteData() {
	t.tree.RemoveByValue([]btree.Comparator{})
}

func (t *Table) deleteDataWhere(cmp []Comparator) []*Result {
	result := make([]*Result, 0)
	deletedKeysMap := make(map[btree.KeyType]interface{})

	comparators := make([]btree.Comparator, 0)
	for _, c := range cmp {
		comparators = append(comparators, c.toBTreeComparator())
	}
	deletedKeys := t.tree.RemoveByValue(comparators)

	for _, key := range deletedKeys {
		result = append(result, newResult(key, nil))
		deletedKeysMap[key] = nil
	}

	for _, index := range t.indexes {
		c := btree.Comparator{
			Operation: "in",
			Value:     deletedKeysMap,
		}
		index.tree.RemoveByValue([]btree.Comparator{c})
	}

	return result
}

func (t *Table) updateData() {

}

func (t *Table) updateDataWhere() {

}
