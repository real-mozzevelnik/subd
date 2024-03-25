package ddl

import (
	"fmt"
	"regexp"
	"subd/internal/db"
	"subd/internal/parser/errors"
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

func (d *DropTable) Prepare() *errors.Error {
	re := regexp.MustCompile(`(\w+)`)
	match := re.FindStringSubmatch(d.request)

	if len(match) != 3 {
		return &errors.Error{
			Msg:  "Invalid request",
			Code: errors.INVALID_REQUEST,
			Req:  d.request,
		}
	}

	d.tableName = match[1]
	if d.tableName == "" {
		err := fmt.Errorf("invalid drop tabe requst: %s", d.tableName)
		panic(err)
	}

	return nil
}

func (d *DropTable) Execute() (resultSet []map[string]interface{}, err *errors.Error) {
	d.dataBase.DropTable(d.tableName)
	return resultSet, nil
}
