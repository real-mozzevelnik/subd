package db

import (
	"runtime"
	"sync"

	"github.com/google/uuid"
)

type Table struct {
	schema      map[string]interface{}
	indexes     map[string]*Index
	dataStorage *DataStorage
	name        string
}

func newTable(name string, schema map[string]interface{}) *Table {
	return &Table{
		name:        name,
		schema:      schema,
		indexes:     make(map[string]*Index),
		dataStorage: NewDataStorage(),
	}
}

func (t *Table) drop() {
	for name, _ := range t.indexes {
		t.dropIndex(name)
	}
	t.dataStorage.DeleteAll()
}

func (t *Table) newIndex(fieldName string) {
	t.indexes[fieldName] = newIndex(fieldName)
	keys := t.dataStorage.ReadAllKeys()
	for _, key := range keys {
		dataKey := t.dataStorage.Read(key).Data[fieldName]
		t.indexes[fieldName].tree.Put(dataKey, key)
	}
}

func (t *Table) dropIndex(fieldName string) {
	t.indexes[fieldName].tree = nil
	delete(t.indexes, fieldName)
	runtime.GC()
}

func (t *Table) selectData() []*Row {
	return t.dataStorage.ReadAll()
}

func (t *Table) selectDataWhere(cmp []Comparator) []*Row {
	return t.dataStorage.ReadAllWhere(
		func(row *Row) bool {
			isOk := true
			for _, comparator := range cmp {
				if !comparator.compare(row) {
					isOk = false
					break
				}
			}
			return isOk
		},
	)
}

func (t *Table) insertData(data map[string]interface{}) {
	rowKey := uuid.NewString()
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		t.dataStorage.Add(rowKey, data)
	}()

	for _, index := range t.indexes {
		wg.Add(1)
		go func(index *Index) {
			defer wg.Done()
			index.tree.Put(data[index.name], rowKey)
		}(index)
	}

	wg.Wait()
}

func (t *Table) deleteData() {
	t.dataStorage.DeleteAll()
}

func (t *Table) deleteDataWhere(cmp []Comparator) {
	t.dataStorage.DeleteAllWhere(
		func(row *Row) bool {
			isOk := true
			for _, comparator := range cmp {
				if !comparator.compare(row) {
					isOk = false
					break
				}
			}
			return isOk
		},
	)
}

// func (t *Table) drop() {
// 	for fieldName, _ := range t.indexes {
// 		t.dropIndex(fieldName)
// 	}
// 	t.deleteData()
// }

// func (t *Table) newIndex(fieldName string) {
// 	t.indexes[fieldName] = newIndex(fieldName)
// 	tableData := t.selectData()
// 	for _, row := range tableData {
// 		t.indexes[fieldName].tree.Put(row.Value.(map[string]interface{})[fieldName], row.Key)
// 	}
// }

// func (t *Table) dropIndex(fieldName string) {
// 	t.indexes[fieldName].tree.RemoveByValue([]btree.Comparator{})
// 	delete(t.indexes, fieldName)
// }

// func (t *Table) selectData() []*Result {
// 	rows := make([]*Result, 0)
// 	items := t.tree.GetByValue([]btree.Comparator{})
// 	for _, item := range items {
// 		result := newResult(item.Key, item.Value)
// 		rows = append(rows, result)
// 	}
// 	return rows
// }

// func (t *Table) selectDataWhere(cmp []Comparator) []*Result {
// 	comparators := make([]btree.Comparator, 0)
// 	for _, c := range cmp {
// 		comparators = append(comparators, c.toBTreeComparator())
// 	}

// 	rows := make([]*Result, 0)
// 	items := t.tree.GetByValue(comparators)
// 	for _, item := range items {
// 		result := newResult(item.Key, item.Value)
// 		rows = append(rows, result)
// 	}
// 	return rows
// }

// func (t *Table) insertData(data map[string]interface{}) {
// 	var wg sync.WaitGroup
// 	rowId := uuid.NewString()

// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		t.tree.Put(btree.KeyType(rowId), data)
// 	}()

// 	for fieldName, index := range t.indexes {
// 		wg.Add(1)
// 		go func(fieldName string) {
// 			defer wg.Done()
// 			index.tree.Put(data[fieldName], rowId)
// 		}(fieldName)
// 	}

// 	wg.Wait()
// }

// func (t *Table) deleteData() {
// 	t.tree.RemoveByValue([]btree.Comparator{})
// }

// func (t *Table) deleteDataWhere(cmp []Comparator) []*Result {
// 	result := make([]*Result, 0)
// 	deletedKeysMap := make(map[btree.KeyType]interface{})

// 	comparators := make([]btree.Comparator, 0)
// 	for _, c := range cmp {
// 		comparators = append(comparators, c.toBTreeComparator())
// 	}
// 	deletedKeys := t.tree.RemoveByValue(comparators)

// 	for _, key := range deletedKeys {
// 		result = append(result, newResult(key, nil))
// 		deletedKeysMap[key] = nil
// 	}

// 	for _, index := range t.indexes {
// 		c := btree.Comparator{
// 			Operation: "in",
// 			Value:     deletedKeysMap,
// 		}
// 		index.tree.RemoveByValue([]btree.Comparator{c})
// 	}

// 	return result
// }

// func (t *Table) updateData() {

// }

// func (t *Table) updateDataWhere() {

// }
