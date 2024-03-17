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

// func (s *Statement) Prepare() {
// fmt.Printf("\nrequest: %s", s.requestText)

// keyWord := strings.Fields(s.requestText)[0]

// switch keyWord {
// case "SELECT":

// 	break

// case "INSERT":
// 	break

// case "DELETE":
// 	break
// }
// }

// func (s *Statement) Execute() {
// 	for _, elem := range s.stmtExprQueue {
// 		elem.Execute()
// 	}
// }

// func (s *Statement) OriginText() string {
// 	return s.requestText
// }
