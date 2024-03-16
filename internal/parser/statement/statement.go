package statement

import (
	"fmt"
	"strings"
)

type Statement struct {
	requestText   string
	stmtExprQueue []StmtExpr
}

func New(request string) *Statement {
	queue := ParseStatement(request)
	return &Statement{
		requestText:   request,
		stmtExprQueue: queue,
	}
}

func (s *Statement) Prepare() {
	fmt.Printf("\nrequest: %s", s.requestText)

	keyWord := strings.Fields(s.requestText)[0]

	switch keyWord {
	case "SELECT":

		break

	case "INSERT":
		break

	case "DELETE":
		break
	}
}

func (s *Statement) Execute() {
	for _, elem := range s.stmtExprQueue {
		elem.Execute()
	}
}

func (s *Statement) OriginText() string {
	return s.requestText
}
