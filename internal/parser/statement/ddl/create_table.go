package ddl

import (
	"fmt"
	"regexp"
	"strings"
	"subd/internal/db"
	"subd/internal/parser/errors"
)

var (
	availableVariables = map[string]bool{
		"INTEGER": true,
		"FLOAT":   true,
		"TEXT":    true,
		"BOOL":    true,
	}
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

func (c *CreateTable) Prepare() *errors.Error {
	re := regexp.MustCompile(`(.*)\s+[\s\(](.*?)\)`)
	match := re.FindStringSubmatch(c.request)

	if len(match) != 3 {
		return &errors.Error{
			Msg:  "Invalid request",
			Code: errors.INVALID_REQUEST,
			Req:  c.request,
		}
	}

	c.tableName = strings.Replace(match[1], " ", "", -1)
	data := strings.Split(match[2], ",")

	for idx := 0; idx < len(data); idx++ {
		fieldType := strings.Fields(data[idx])

		if len(fieldType) != 2 {
			return &errors.Error{
				Msg:  fmt.Sprintf("incorrect expression: %s", fieldType),
				Code: errors.INVALID_REQUEST,
				Req:  "create table" + c.request,
			}
		}

		switch fieldType[1] {
		case "INTEGER", "FLOAT", "TEXT", "BOOL",
			"integer", "float", "text", "bool":
			c.schema[fieldType[0]] = fieldType[1]

		default:
			return &errors.Error{
				Msg:  fmt.Sprintf("unknown type of variable: %s", fieldType[1]),
				Code: errors.INVALID_REQUEST,
				Req:  "create table" + c.request,
			}
		}
	}

	return nil
}

func (c *CreateTable) Execute() (resultSet []map[string]interface{}, err *errors.Error) {
	c.dataBase.CreateTable(c.tableName, c.schema)
	return resultSet, nil
}
