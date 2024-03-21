package db

import (
	"fmt"
	"runtime"
	"subd/internal/utils"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/google/uuid"
)

type table struct {
	schema      map[string]interface{}
	indexes     map[string]*index
	dataStorage *dataStorage
	name        string
}

func newTable(name string, schema map[string]interface{}) *table {
	return &table{
		name:        name,
		schema:      schema,
		indexes:     make(map[string]*index),
		dataStorage: newDataStorage(),
	}
}

func (t *table) drop() {
	for name, _ := range t.indexes {
		t.dropIndex(name)
	}
	t.dataStorage.DeleteAll()
}

func (t *table) newIndex(fieldName string) {
	t.indexes[fieldName] = newIndex(fieldName)
	keys := t.dataStorage.ReadAllKeys()
	for _, key := range keys {
		dataKey := t.dataStorage.Read(key).Data[fieldName]
		t.indexes[fieldName].tree.Put(dataKey, key)
	}
}

func (t *table) dropIndex(fieldName string) {
	t.indexes[fieldName].tree = nil
	delete(t.indexes, fieldName)
	runtime.GC()
}

func (t *table) selectData(searchedFields []string) []map[string]interface{} {
	return t.dataStorage.ReadAll(searchedFields)
}

func (t *table) selectDataWhere(cmp []utils.Comparator, searchedFields []string) []map[string]interface{} {
	start := time.Now()

	areIndexesUsed := false
	for _, comp := range cmp {
		_, ok := t.indexes[comp.FieldName]
		if ok {
			areIndexesUsed = true
			break
		}
	}
	if len(t.indexes) == 0 || !areIndexesUsed {
		return t.dataStorage.ReadAllWhere(
			func(row *Row) bool {
				isOk := true
				for _, comparator := range cmp {
					if !comparator.Compare(row.Data[comparator.FieldName]) {
						isOk = false
						break
					}
				}
				return isOk
			},
			searchedFields,
		)
	}

	comporatorsSet := mapset.NewSet[*utils.Comparator]()
	keySet, usedComporatorsFields := t.searchInIndexes(cmp)
	for _, comporator := range cmp {
		if !usedComporatorsFields.ContainsOne(comporator.FieldName) {
			comporatorsSet.Add(&comporator)
		}
	}
	unusedCmp := comporatorsSet.ToSlice()

	r := t.dataStorage.ReadAllWhereWithGivenKeys(
		func(row *Row) bool {
			isOk := true
			for _, comparator := range unusedCmp {
				if !comparator.Compare(row.Data[comparator.FieldName]) {
					isOk = false
					break
				}
			}
			return isOk
		},
		searchedFields,
		keySet.ToSlice(),
	)

	elapsed := time.Since(start)
	fmt.Println(elapsed)
	return r
}

func (t *table) insertData(data map[string]interface{}) {
	rowKey := uuid.NewString()
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		t.dataStorage.Add(rowKey, data)
	}()

	for _, idx := range t.indexes {
		wg.Add(1)
		go func(idx *index) {
			defer wg.Done()
			idx.tree.Put(data[idx.name], rowKey)
		}(idx)
	}

	wg.Wait()
}

func (t *table) deleteData() {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		t.dataStorage.DeleteAll()
	}()

	for fieldName, _ := range t.indexes {
		wg.Add(1)
		go func(fieldName string) {
			defer wg.Done()
			t.dropIndex(fieldName)
			t.newIndex(fieldName)
		}(fieldName)
	}

	wg.Wait()
}

func (t *table) deleteDataWhere(cmp []utils.Comparator) {
	deletedKeys := t.dataStorage.DeleteAllWhere(
		func(row *Row) bool {
			isOk := true
			for _, comparator := range cmp {
				if !comparator.Compare(row.Data[comparator.FieldName]) {
					isOk = false
					break
				}
			}
			return isOk
		},
	)

	if len(t.indexes) == 0 {
		return
	}
	var wg sync.WaitGroup
	for _, idx := range t.indexes {
		wg.Add(1)
		go func(idx *index) {
			defer wg.Done()
			idx.tree.RemoveWithValues(deletedKeys)
		}(idx)
	}

	wg.Wait()
}

func (t *table) searchInIndexes(cmp []utils.Comparator) (keySet mapset.Set[string], usedComporatorsFields mapset.Set[string]) {
	keySet = mapset.NewSet[string]()
	isKeySetEmpty := true
	usedComporatorsFields = mapset.NewSet[string]()

	var wg sync.WaitGroup
	for fieldName, idx := range t.indexes {
		wg.Add(1)
		go func(fieldName string, idx *index) {
			defer wg.Done()
			indexComparators := make([]*utils.Comparator, 0)
			for _, comparator := range cmp {
				if comparator.FieldName == fieldName {
					indexComparators = append(indexComparators, &comparator)
				}
			}

			for _, comporator := range indexComparators {
				usedComporatorsFields.Add(comporator.FieldName)
			}

			if isKeySetEmpty {
				isKeySetEmpty = false
				keySet.Append(idx.tree.GetWithConditions(indexComparators).ToSlice()...)
			} else {
				keySet.Intersect(idx.tree.GetWithConditions(indexComparators))
			}
		}(fieldName, idx)
	}

	wg.Wait()
	return keySet, usedComporatorsFields
}
