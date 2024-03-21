package dql

import (
	"fmt"
	"regexp"
	"subd/internal/db"
)

type CreateIndex struct {
	dataBase  *db.DB
	request   string
	tableName string
	fieldName string
}

func NewCreateIndex(dataBase *db.DB, request string) *CreateIndex {
	return &CreateIndex{
		dataBase: dataBase,
		request:  request,
	}
}

func (d *CreateIndex) Prepare() {
	re := regexp.MustCompile(`(\w+)\s(?i)ON\s(\w+)`)
	match := re.FindStringSubmatch(d.request)

	fmt.Println("create index")
	for i, m := range match {
		fmt.Printf("%d: %s\n", i, m)
	}

	d.tableName = match[1]
	d.fieldName = match[2]
}

func (d *CreateIndex) Execute() []map[string]interface{} {
	d.dataBase.CreateIndex(d.tableName, d.fieldName)
	return nil
}
