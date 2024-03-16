package dql

type Select struct {
	Expr string
}

func New(request string) *Select {
	return &Select{
		Expr: request,
	}
}

func (s *Select) Execute() error {

	return nil
}

var SelectExecOrder = map[string]int{
	"JOIN":     1,
	"FROM":     2,
	"WHERE":    3,
	"GROUP BY": 4,
	"HAVING":   5,
	"SELECT":   6,
	"LIMIT":    7,
}
