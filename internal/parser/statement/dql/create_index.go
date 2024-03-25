package dql

import (
	"regexp"
	"subd/internal/db"
	"subd/internal/parser/errors"
)

type CreateIndex struct {
	dataBase  *db.DB
	request   string
	tableName string
	fieldName string
}

func NewCreateIndex(dataBase *db.DB, request string) (newIndex *CreateIndex) {
	return &CreateIndex{
		dataBase: dataBase,
		request:  request,
	}
}

func (d *CreateIndex) Prepare() *errors.Error {
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

	return nil
}

func (d *CreateIndex) Execute() (resultSet []map[string]interface{}, err *errors.Error) {
	d.dataBase.CreateIndex(d.tableName, d.fieldName)
	return resultSet, nil
}
