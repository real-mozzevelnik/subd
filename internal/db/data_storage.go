package db

import (
	"sync"
)

type dataStorage struct {
	Collection map[string]*Row
	Mutex      sync.Mutex
}

func newDataStorage() *dataStorage {
	return &dataStorage{
		Collection: make(map[string]*Row),
	}
}

func (d *dataStorage) Add(key string, data map[string]interface{}) {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	d.Collection[key] = newRow(data)
}

func (d *dataStorage) ReadAll(searchedFields []string) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)

	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	for _, data := range d.Collection {
		dataWithSearchedFields := make(map[string]interface{})
		for _, searchedField := range searchedFields {
			dataWithSearchedFields[searchedField] = data.Data[searchedField]
		}
		result = append(result, dataWithSearchedFields)
	}

	return result
}

func (d *dataStorage) ReadAllWhere(where func(row *Row) bool, searchedFields []string) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)

	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	for _, data := range d.Collection {
		if where(data) {
			dataWithSearchedFields := make(map[string]interface{})
			for _, searchedField := range searchedFields {
				dataWithSearchedFields[searchedField] = data.Data[searchedField]
			}
			result = append(result, dataWithSearchedFields)
		}
	}

	return result
}

func (d *dataStorage) ReadAllKeys() []string {
	result := make([]string, 0)

	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	for key, _ := range d.Collection {
		result = append(result, key)
	}

	return result
}

func (d *dataStorage) Read(key string) *Row {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	res, ok := d.Collection[key]
	if !ok {
		return nil
	}

	return res
}

func (d *dataStorage) DeleteAll() {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	for key, _ := range d.Collection {
		delete(d.Collection, key)
	}
}

func (d *dataStorage) DeleteAllWhere(where func(row *Row) bool) (deletedKeys []string) {
	deletedKeys = make([]string, 0)

	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	for key, data := range d.Collection {
		if where(data) {
			delete(d.Collection, key)
			deletedKeys = append(deletedKeys, key)
		}
	}
	return deletedKeys
}

func (d *dataStorage) Delete(key string) {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	delete(d.Collection, key)
}
