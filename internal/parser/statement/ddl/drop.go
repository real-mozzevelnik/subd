package ddl

import (
	"regexp"
	"subd/internal/db"
)

type Drop struct {
	dataBase  *db.DB
	request   string
	tableName string
}

func NewDrop(db *db.DB, req string) *Drop {
	return &Drop{
		dataBase: db,
		request:  req,
	}
}

func (d *Drop) Prepare() {
	re := regexp.MustCompile(`(?i)DROP\s+(?i)TABLE\s+(.*)`)
	match := re.FindStringSubmatch(d.request)

	d.tableName = match[1]
	if d.tableName == "" {
		panic("Empty table name")
	}
}

func (d *Drop) Execute() []map[string]interface{} {
	d.dataBase.DropTable(d.tableName)
	return nil
}
