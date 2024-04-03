package ddl

import (
	"fmt"
	"strings"
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
	raw := strings.Fields(d.request)
	if len(raw) != 1 {
		return &errors.Error{
			Msg:  fmt.Sprintf("Invalid request: unknown instructions: %s", raw[1:]),
			Code: errors.INVALID_REQUEST,
			Req:  "drop table" + d.request,
		}
	}

	d.tableName = raw[0]

	// adding check is this table exist

	if d.tableName == "" {
		return &errors.Error{
			Msg:  "table name isn't exist",
			Code: errors.INVALID_REQUEST,
			Req:  "drop table" + d.request,
		}
	}

	return nil
}

func (d *DropTable) Execute() (resultSet []map[string]interface{}, err *errors.Error) {
	d.dataBase.DropTable(d.tableName)
	return resultSet, nil
}
