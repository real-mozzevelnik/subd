package dml

import "subd/internal/db"

type Update struct {
	DataBase *db.DB
	Request  string
}

func (i *Update) Prepare() {

}

func (i *Update) Execute() {

}
