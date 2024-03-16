package statement

type Statement struct {
	requestText   string
	stmtExprQueue []StmtExpr
}

// func (s *Statement) New(request string) {
// 	s.requestText = request
// 	s.stmtExprQueue = ParseStatement(request)
// }

func New(request string) *Statement {
	queue := ParseStatement(request)
	return &Statement{
		requestText:   request,
		stmtExprQueue: queue,
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
