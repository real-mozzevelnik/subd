package statement

// type StmtExpr interface {
// 	Execute() error
// }

// func ParseStatement(request string) []StmtExpr {

// 	return nil
// }

// func CreateQuery(queryText string) StmtExpr {
// 	queryType := strings.Fields(queryText)[0]

// 	var query StmtExpr

// 	switch strings.ToLower(queryType) {
// 	case "DROP":
// 		query = &ddl.Drop{
// 			Expr: queryText,
// 		}
// 	case "CREATE":
// 		query = &ddl.Create{
// 			Expr: queryText,
// 		}
// 	case "SELECT":
// 		query = &dql.Select{
// 			Expr: queryText,
// 		}
// 	}

// 	return query
// }
