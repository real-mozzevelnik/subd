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
	Execute() []map[string]interface{}
}

func New(request string, database *db.DB) *Statement {
	keyWord := strings.Fields(request)[0]
	var statement Statement

	switch strings.ToLower(keyWord) {
	case "select":
		statement = dql.NewSelect(database, request)
	case "insert":
		statement = dml.NewInsert(database, request)
	case "delete":
		statement = dml.NewDelete(database, request)
	case "update":
		statement = dml.NewUpdate(database, request)
	case "create":
		statement = ddl.NewCreate(database, request)
	case "drop":
		statement = ddl.NewDrop(database, request)
	}

	return &statement
}
