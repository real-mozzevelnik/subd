package db

import "sync"

type DataStorage struct {
	Collection map[string]*Row
	Mutex      sync.Mutex
}

func NewDataStorage() *DataStorage {
	return &DataStorage{
		Collection: make(map[string]*Row),
	}
}

func (d *DataStorage) Add(key string, data map[string]interface{}) {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	d.Collection[key] = NewRow(data)
}

func (d *DataStorage) ReadAll() []*Row {
	result := make([]*Row, 0)

	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	for _, data := range d.Collection {
		result = append(result, data)
	}

	return result
}

func (d *DataStorage) ReadAllWhere(where func(row *Row) bool) []*Row {
	result := make([]*Row, 0)

	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	for _, data := range d.Collection {
		if where(data) {
			result = append(result, data)
		}
	}

	return result
}

func (d *DataStorage) ReadAllKeys() []string {
	result := make([]string, 0)

	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	for key, _ := range d.Collection {
		result = append(result, key)
	}

	return result
}

func (d *DataStorage) Read(key string) *Row {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	res, ok := d.Collection[key]
	if !ok {
		return nil
	}

	return res
}

func (d *DataStorage) DeleteAll() {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	for key, _ := range d.Collection {
		delete(d.Collection, key)
	}
}

func (d *DataStorage) DeleteAllWhere(where func(row *Row) bool) {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	for key, data := range d.Collection {
		if where(data) {
			delete(d.Collection, key)
		}
	}
}

func (d *DataStorage) Delete(key string) {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	delete(d.Collection, key)
}
