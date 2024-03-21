package statement

import (
	"fmt"
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
	keyWords := strings.Fields(request)[:2]
	var statement Statement

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
		err := fmt.Errorf("invalid request: %s", request)
		panic(err)
	}

	// fmt.Println(request[20:])

	return &statement
}
