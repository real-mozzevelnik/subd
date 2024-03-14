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

func (db *DB) GetTableSchema(name string) map[string]interface{} {
	return db.tables[name].schema
}

func (db *DB) DropTable(name string) {
	db.tables[name].drop()
	delete(db.tables, name)
}

func (db *DB) CreateIndex(tableName, fieldName string) {
	db.tables[tableName].newIndex(fieldName)
}

func (db *DB) DropIndex(tableName, fieldName string) {
	db.tables[tableName].dropIndex(fieldName)
}

func (db *DB) Select(tableName string) []*Result {
	return db.tables[tableName].selectData()
}

func (db *DB) SelectWhere(tableName string, cmp []Comparator) []*Result {
	return db.tables[tableName].selectDataWhere(cmp)
}

func (db *DB) Insert(tableName string, data map[string]interface{}) {
	db.tables[tableName].insertData(data)
}

func (db *DB) Delete(tableName string) {
	db.tables[tableName].deleteData()
}

func (db *DB) DeleteWhere(tableName string, cmp []Comparator) {
	db.tables[tableName].deleteDataWhere(cmp)
}
