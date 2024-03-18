package dml

import (
	"regexp"
	"strings"
	"subd/internal/db"
)

type Insert struct {
	dataBase  *db.DB
	request   string
	tableName string
	data      map[string]interface{}
}

func NewInsert(db *db.DB, req string) *Insert {
	return &Insert{
		dataBase: db,
		request:  req,
	}
}

func (i *Insert) Prepare() {
	// BUGS: "INTO users()" calls panic, although "INTO users ()" will work correctly
	re := regexp.MustCompile(`(?i)INSERT\s+(?i)INTO\s+(.*?)\s*[\s\(](.*?)\)\s*(?i)VALUES\s*[\s\(](.*)\)`)
	match := re.FindStringSubmatch(i.request)

	i.tableName = match[1]

	columnsStr := match[2]
	columns := strings.Split(columnsStr, ",")

	valuesStr := match[3]
	values := strings.Split(valuesStr, ",")

	tableSchema := i.dataBase.GetTableSchema(i.tableName)
	replacer := strings.NewReplacer("\"", "", "'", "")

	i.data = make(map[string]interface{})
	for idx, column := range columns {
		switch tableSchema[column] {
		case "TEXT":
			if values[idx][0] == '"' || values[idx][0] == '\'' {
				values[idx] = replacer.Replace(values[idx])
				i.data[column] = values[idx]
			} else {
				panic("not text")
			}

		case "INTEGER":
			if values[idx][0] != '"' && values[idx][0] != '\'' {
				i.data[column] = values[idx]
			} else {
				panic("not num")
			}

		case "FALSE":

		}

	}
}

func (i *Insert) Execute() []*db.Row {
	i.dataBase.Insert(i.tableName, i.data)
	return nil
}
