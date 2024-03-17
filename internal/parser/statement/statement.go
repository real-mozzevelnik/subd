package statement

import (
	"strings"
	"subd/internal/db"
	"subd/internal/parser/statement/ddl"
	"subd/internal/parser/statement/dml"
	"subd/internal/parser/statement/dql"
)

type Statement interface {
	Prepare()
	Execute()
}

func New(request string, database *db.DB) *Statement {
	keyWord := strings.Fields(request)[0]
	var statement Statement

	switch strings.ToLower(keyWord) {
	case "select":
		statement = &dql.Select{
			Request:  request,
			DataBase: database,
		}
	case "insert":
		statement = &dml.Insert{
			Request:  request,
			DataBase: database,
		}
	case "delete":
		statement = &dml.Delete{
			Request:  request,
			DataBase: database,
		}
	case "update":
		statement = &dml.Update{
			DataBase: database,
			Request:  request,
		}
	case "drop":
		statement = &ddl.Drop{
			DataBase: database,
			Request:  request,
		}
	}

	return &statement
}
