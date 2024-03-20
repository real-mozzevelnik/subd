package ddl

import (
	"fmt"
	"regexp"
	"strings"
	"subd/internal/db"
)

type Create struct {
	dataBase  *db.DB
	request   string
	tableName string
	schema    map[string]interface{}
}

func NewCreate(db *db.DB, req string) *Create {
	return &Create{
		dataBase: db,
		request:  req,
		schema:   make(map[string]interface{}),
	}
}

func (c *Create) Prepare() {
	re := regexp.MustCompile(`(?i)CREATE\s+(?i)TABLE\s+(.*?)\s*[\s\(](.*?)\)`)

	match := re.FindStringSubmatch(c.request)

	if len(match) < 3 {
		fmt.Println(len(match))
		for idx, d := range match {
			fmt.Println(idx, d)
		}
		panic("invalid table creation request")
	}

	c.tableName = match[1]

	data := strings.Split(match[2], ",")

	for idx := 0; idx < len(data); idx++ {
		fieldAndType := strings.Fields(data[idx])
		c.schema[fieldAndType[0]] = fieldAndType[1]
	}

	fmt.Printf("create table <%s> with: %v\n\n", c.tableName, data)
}

func (c *Create) Execute() []map[string]interface{} {
	c.dataBase.Createtable(c.tableName, c.schema)
	return nil
}
