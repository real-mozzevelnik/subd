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
	// BUGS: "INTO users()" calls panic, although "INTO users ()" will work correctly
	re := regexp.MustCompile(`INSERT INTO (.*) \((.*)\) VALUES\s*[\s\(](.*)\)`)
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

}

func (i *Insert) Execute() []*db.Row {
	i.DataBase.Insert(i.tableName, i.data)

	// LOGS BLOCK
	{
		fmt.Printf("\n\n%v\n%v\n", i.Request, i.data)
	}
	return nil
}
