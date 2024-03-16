package statement

import (
	"strings"

	"subd/internal/parser/statement/ddl"
	"subd/internal/parser/statement/dql"
)

type StmtExpr interface {
	Execute() error
}

func ParseStatement(request string) []StmtExpr {

	return nil
}

func CreateQuery(queryText string) StmtExpr {
	queryType := strings.Fields(queryText)[0]

	var query StmtExpr

	switch strings.ToLower(queryType) {
	case "DROP":
		query = &ddl.Drop{
			Expr: queryText,
		}
	case "CREATE":
		query = &ddl.Create{
			Expr: queryText,
		}
	case "SELECT":
		query = &dql.Select{
			Expr: queryText,
		}
	}

	return query
}

// const statements = `DENY GRANT REVOKE ALTER CREATE DROP RENAME TRUNCATE DELETE INSERT MERGE
// UPDATE CASE FROM GROUP BY HAVING JOIN LIMIT ORDER BY SELECT USING WHERE`

const (
	CREATE = 1
	FROM   = 2
)
