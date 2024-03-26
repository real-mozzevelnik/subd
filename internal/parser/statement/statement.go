package statement

import (
	"strings"
	"subd/internal/db"
	"subd/internal/parser/errors"
	"subd/internal/parser/statement/ddl"
	"subd/internal/parser/statement/dml"
	"subd/internal/parser/statement/dql"
	"subd/internal/utils"
)

type Statement interface {
	Prepare() (err *errors.Error)
	Execute() (resultSet []map[string]interface{}, err *errors.Error)
}

func New(request string, database *db.DB) (newStatement *Statement, err *errors.Error) {
	var statement Statement
	keyWords := utils.FieldsN(request, 2)

	switch strings.ToLower(keyWords[0]) {
	case "select":
		statement = dql.NewSelect(database, request[strings.Index(request, keyWords[0])+6:])

	case "insert":
		statement = dml.NewInsert(database, request[strings.Index(request, keyWords[1])+4:])

	case "delete":
		statement = dml.NewDelete(database, request[strings.Index(request, keyWords[0])+4:])

	case "update":
		statement = dml.NewUpdate(database, request[strings.Index(request, keyWords[0])+6:])

	case "create":
		switch strings.ToLower(keyWords[1]) {
		case "table":
			statement = ddl.NewCreateTable(database, request[strings.Index(request, keyWords[1])+6:])

		case "index":
			statement = dql.NewCreateIndex(database, request[strings.Index(request, keyWords[1])+6:])
		}

	case "drop":
		switch strings.ToLower(keyWords[1]) {
		case "table":
			statement = ddl.NewDropTable(database, request[strings.Index(request, keyWords[1])+6:])

		case "index":
			statement = dql.NewDropIndex(database, request[strings.Index(request, keyWords[1])+6:])
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
