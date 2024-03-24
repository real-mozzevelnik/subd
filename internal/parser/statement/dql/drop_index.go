package dql

import (
	"regexp"
	"subd/internal/db"
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

func (d *DropIndex) Prepare() (err error) {
	re := regexp.MustCompile(`(\w+)\s(?i)ON\s(\w+)`)
	match := re.FindStringSubmatch(d.request)

	d.tableName = match[1]
	d.fieldName = match[2]

	return err
}

func (d *DropIndex) Execute() (resultSet []map[string]interface{}, err error) {
	d.dataBase.DropIndex(d.tableName, d.fieldName)

	return resultSet, err
}
