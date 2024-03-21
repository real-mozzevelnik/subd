package ddl

import (
	"fmt"
	"regexp"
	"strings"
	"subd/internal/db"
)

type CreateTable struct {
	dataBase  *db.DB
	request   string
	tableName string
	schema    map[string]interface{}
}

func NewCreateTable(db *db.DB, req string) *CreateTable {
	return &CreateTable{
		dataBase: db,
		request:  req,
		schema:   make(map[string]interface{}),
	}
}

func (c *CreateTable) Prepare() {
	re := regexp.MustCompile(`(.*?)\s+[\s\(](.*?)\)`)
	match := re.FindStringSubmatch(c.request)

	if len(match) != 3 {
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

	// fmt.Printf("create table <%s> with data: %v\n\n", c.tableName, data)
}

func (c *CreateTable) Execute() []map[string]interface{} {
	c.dataBase.Createtable(c.tableName, c.schema)
	return nil
}
