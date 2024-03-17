package dml

import (
	"fmt"
	"regexp"
	"strings"
	"subd/internal/db"
)

type Insert struct {
	DataBase  *db.DB
	Request   string
	tableName string
	data      map[string]interface{}
}

func (i *Insert) Prepare() {
	re := regexp.MustCompile(`INSERT INTO (.*) \((.*)\) VALUES\((.*)\)`)
	match := re.FindStringSubmatch(i.Request)

	i.tableName = match[1]

	columnsStr := match[2]
	columns := strings.Split(columnsStr, ",")

	valuesStr := match[3]
	values := strings.Split(valuesStr, ",")

	i.data = make(map[string]interface{})
	for idx, column := range columns {
		i.data[column] = values[idx]
	}

	fmt.Printf("%v\n%v\n\n", i.Request, i.data)
}

func (i *Insert) Execute() {
	i.DataBase.Insert(i.tableName, i.data)
}
