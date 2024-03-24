package ddl

import (
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

func (c *CreateTable) Prepare() error {
	re := regexp.MustCompile(`(.*?)\s+[\s\(](.*?)\)`)
	match := re.FindStringSubmatch(c.request)

	c.tableName = match[1]

	data := strings.Split(match[2], ",")

	for idx := 0; idx < len(data); idx++ {
		fieldAndType := strings.Fields(data[idx])
		c.schema[fieldAndType[0]] = fieldAndType[1]
	}

	return nil
}

func (c *CreateTable) Execute() (resultSet []map[string]interface{}, err error) {
	c.dataBase.CreateTable(c.tableName, c.schema)
	return resultSet, err
}
