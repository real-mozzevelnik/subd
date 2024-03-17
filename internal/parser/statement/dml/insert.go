package dml

import "subd/internal/db"

type Insert struct {
	DataBase *db.DB
	Request  string
}

func (i *Insert) Prepare() {

}

func (i *Insert) Execute() {

}
