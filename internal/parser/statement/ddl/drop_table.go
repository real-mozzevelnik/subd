package ddl

import (
	"fmt"
	"regexp"
	"subd/internal/db"
)

type DropTable struct {
	dataBase  *db.DB
	request   string
	tableName string
}

func NewDropTable(db *db.DB, req string) *DropTable {
	return &DropTable{
		dataBase: db,
		request:  req,
	}
}

func (d *DropTable) Prepare() {
	re := regexp.MustCompile(`(\w+)`)
	match := re.FindStringSubmatch(d.request)

	d.tableName = match[1]
	if d.tableName == "" {
		err := fmt.Errorf("invalid drop tabe requst: %s", d.tableName)
		panic(err)
	}

	// fmt.Printf("drop table <%s>", d.tableName)
}

func (d *DropTable) Execute() []map[string]interface{} {
	d.dataBase.DropTable(d.tableName)
	return nil
}
