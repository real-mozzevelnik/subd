package statement

import (
	"strings"
	"subd/internal/db"
	"subd/internal/parser/errors"
	"subd/internal/parser/statement/ddl"
	"subd/internal/parser/statement/dml"
	"subd/internal/parser/statement/dql"
)

type Statement interface {
	Prepare() (err *errors.Error)
	Execute() (resultSet []map[string]interface{}, err *errors.Error)
}

func New(request string, database *db.DB) (newStatement *Statement, err *errors.Error) {
	var statement Statement
	keyWords := strings.Fields(request)[:2]

	switch strings.ToLower(keyWords[0]) {
	case "select":
		statement = dql.NewSelect(database, request[7:])

	case "insert":
		statement = dml.NewInsert(database, request[12:])

	case "delete":
		statement = dml.NewDelete(database, request[12:])

	case "update":
		statement = dml.NewUpdate(database, request[7:])

	case "create":
		switch strings.ToLower(keyWords[1]) {
		case "table":
			statement = ddl.NewCreateTable(database, request[13:])

		case "index":
			statement = dql.NewCreateIndex(database, request[13:])
		}

	case "drop":
		switch strings.ToLower(keyWords[1]) {
		case "table":
			statement = ddl.NewDropTable(database, request[11:])

		case "index":
			statement = dql.NewDropIndex(database, request[11:])
		}
	}

	if statement == nil {
		return nil, &errors.Error{
			Msg:  "Unknown key words",
			Code: errors.INVALID_REQUEST,
			Req:  request,
		}
	}

	return &statement, nil
}
