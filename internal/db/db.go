package db

type DB struct {
	tables map[string]*Table
}

func NewDB() *DB {
	return &DB{
		tables: make(map[string]*Table),
	}
}

func (db *DB) CreateTable(name string, schema map[string]interface{}) {
	db.tables[name] = newTable(name, schema)
}

func (db *DB) GetSchema(name string) map[string]interface{} {
	return db.tables[name].schema
}

func (db *DB) Select(tableName string) []map[string]interface{} {
	return db.tables[tableName].selectData()
}

func (db *DB) SelectWhere(tableName string, cmp []Comparator) []map[string]interface{} {
	return db.tables[tableName].selectDataWhere(cmp)
}

func (db *DB) Insert(tableName string, data map[string]interface{}) {
	db.tables[tableName].insertData(data)
}
