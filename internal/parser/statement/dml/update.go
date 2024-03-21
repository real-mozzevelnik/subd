package dml

import (
	"fmt"
	"regexp"
	"strings"
	"subd/internal/db"
)

type Update struct {
	dataBase  *db.DB
	request   string
	tableName string
	data      map[string]interface{}
}

func NewUpdate(db *db.DB, req string) *Update {
	return &Update{
		dataBase: db,
		request:  req,
		data:     make(map[string]interface{}, 0),
	}
}

func (d *Update) Prepare() {
	// re := regexp.MustCompile(`(?i)UPDATE\s+([^ ]*)\s+(?i)SET\s+(.*?)(?:\s+(?i)WHERE\s+(.*))?$`)
	re := regexp.MustCompile(`([^ ]*)\s+(?i)SET\s+(.*?)(?:\s+(?i)WHERE\s+(.*))?$`)
	match := re.FindStringSubmatch(d.request)

	fmt.Println(len(match))
	for idx, m := range match {
		fmt.Printf("%d: %v\n", idx, m)
	}

	var err error
	// replacer := strings.NewReplacer("'", "", "\"", "")
	if len(match) < 3 {
		err = fmt.Errorf("invalid update request: %s", match[0])
		panic(err)
	}

	d.tableName = match[1]

	rowData := strings.Split(match[2], ",")
	for idx, d := range rowData {
		fmt.Printf("%d, %v\n", idx, d)
	}
	fmt.Println(rowData)
}

func (d *Update) Execute() []map[string]interface{} {

	return nil
}
