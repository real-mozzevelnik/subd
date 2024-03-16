package ddl

type Drop struct {
	Expr string
}

func (s *Drop) Execute() error {
	s.Expr = ""
	return nil
}
