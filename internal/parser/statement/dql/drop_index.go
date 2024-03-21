package dql

import (
	"fmt"
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

func (d *DropIndex) Prepare() {
	re := regexp.MustCompile(`(\w+)\s(?i)ON\s(\w+)`)
	match := re.FindStringSubmatch(d.request)

	fmt.Println("drop index")
	for i, m := range match {
		fmt.Printf("%d: %s\n", i, m)
	}

	d.tableName = match[1]
	d.fieldName = match[2]
}

func (d *DropIndex) Execute() []map[string]interface{} {
	d.dataBase.DropIndex(d.tableName, d.fieldName)
	return nil
}
