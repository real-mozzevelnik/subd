package dql

import (
	"regexp"
	"subd/internal/db"
	"subd/internal/parser/errors"
)

type DropIndex struct {
	dataBase  *db.DB
	request   string
	tableName string
	fieldName string
}

func NewDropIndex(dataBase *db.DB, request string) *DropIndex {
	return &DropIndex{
		dataBase: dataBase,
		request:  request,
	}
}

func (d *DropIndex) Prepare() *errors.Error {
	re := regexp.MustCompile(`(\w+)\s(?i)ON\s(\w+)`)
	match := re.FindStringSubmatch(d.request)

	if len(match) != 3 {
		return &errors.Error{
			Msg:  "Invalid request",
			Code: errors.INVALID_REQUEST,
			Req:  d.request,
		}
	}

	d.tableName = match[1]
	d.fieldName = match[2]

	_, ok := d.dataBase.GetTableSchema(d.tableName)[d.fieldName]
	if !ok {
		return &errors.Error{
			Msg:  "Unknown table: <" + d.tableName + "> or field: <" + d.fieldName + "> name",
			Code: errors.NOT_FOUND_DATA,
			Req:  d.request,
		}
	}

	return nil
}

func (d *DropIndex) Execute() (resultSet []map[string]interface{}, err *errors.Error) {
	d.dataBase.DropIndex(d.tableName, d.fieldName)
	return resultSet, nil
}
