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

func (d *DropTable) Prepare() error {
	re := regexp.MustCompile(`(\w+)`)
	match := re.FindStringSubmatch(d.request)

	d.tableName = match[1]
	if d.tableName == "" {
		err := fmt.Errorf("invalid drop tabe requst: %s", d.tableName)
		panic(err)
	}

	// fmt.Printf("drop table <%s>", d.tableName)
	return nil
}

func (d *DropTable) Execute() (resultSet []map[string]interface{}, err error) {
	d.dataBase.DropTable(d.tableName)
	return resultSet, err
}
