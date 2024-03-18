package dml

import "subd/internal/db"

type Update struct {
	dataBase *db.DB
	request  string
}

func NewUpdate(db *db.DB, req string) *Update {
	return &Update{
		dataBase: db,
		request:  req,
	}
}

func (i *Update) Prepare() {

}

func (i *Update) Execute() []*db.Row {

	return nil
}
