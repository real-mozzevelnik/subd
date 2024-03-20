package dml

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
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
		data:     make(map[string]interface{}),
	}
}

func (i *Insert) Prepare() {
	re := regexp.MustCompile(`(?i)INSERT\s+(?i)INTO\s+(.*?)\s*[\s\(](.*?)\)\s*(?i)VALUES\s*[\s\(](.*)\)`)
	match := re.FindStringSubmatch(i.request)

	i.tableName = match[1]
	columns := strings.Split(match[2], ",")

	values := strings.Split(match[3], ",")

	tableSchema := i.dataBase.GetTableSchema(i.tableName)
	replacer := strings.NewReplacer("\"", "", "'", "")

	var err error
	for idx, column := range columns {
		switch tableSchema[column] {
		case "TEXT":
			{
				if values[idx][0] == '"' || values[idx][0] == '\'' {
					values[idx] = replacer.Replace(values[idx])
					i.data[column] = values[idx]
				} else {
					err = fmt.Errorf("col <%s> has text type, but value isn't text: %v", column, values[idx])
				}
			}

		case "INTEGER":
			{
				if values[idx][0] != '"' && values[idx][0] != '\'' {
					// parse string to integer
					i.data[column], _ = strconv.ParseInt(values[idx], 10, 0)
				} else {
					err = fmt.Errorf("col <%s> has int type, but value isn't int: %v", column, values[idx])
				}
			}

		default:
			{
				err = fmt.Errorf("col <%s> has invalid type: %v", column, tableSchema[column])
			}
		}
	}

	if err != nil {
		panic(err)
	}

	// temporary measure
	fmt.Println("table name: ", i.tableName)
	fmt.Println("table schema: ", tableSchema)
	fmt.Println("collumns: ", columns)
	fmt.Println("values: ", values)

	for _, d := range i.data {
		fmt.Printf("table <%s>, insert <%v> type <%v>\n", i.tableName, d, reflect.TypeOf(d))
	}
	fmt.Println()
}

func (i *Insert) Execute() []map[string]interface{} {
	i.dataBase.Insert(i.tableName, i.data)
	return nil
}
