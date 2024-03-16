package ddl

type Create struct {
	Expr string
}

func (s *Create) Execute() error {
	s.Expr = ""
	return nil
}
