package dql

type Join interface {
	iJoin()
}

/*

	//	INNER JOIN //

*/

type InnerJoin struct {
	Expr string
}

func (j *InnerJoin) iJoin() {
}

/*

	// LEFT JOIN //

*/

type LeftJoin struct {
	Expr string
}

func (j *LeftJoin) iJoin() {

}

/*

	// RIGHT JOIN //

*/

type RightJoin struct {
	Expr string
}

func (j *RightJoin) iJoin() {

}
