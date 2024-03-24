package dql

import (
	"regexp"
	"subd/internal/db"
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

func (d *CreateIndex) Prepare() (err error) {
	re := regexp.MustCompile(`(\w+)\s(?i)ON\s(\w+)`)
	match := re.FindStringSubmatch(d.request)

	d.tableName = match[1]
	d.fieldName = match[2]

	return err
}

func (d *CreateIndex) Execute() (resultSet []map[string]interface{}, err error) {
	d.dataBase.CreateIndex(d.tableName, d.fieldName)
	return resultSet, err
}
