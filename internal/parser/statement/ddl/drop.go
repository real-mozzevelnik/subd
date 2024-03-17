package ddl

import "subd/internal/db"

type Drop struct {
	DataBase *db.DB
	Request  string
}

func (d *Drop) Prepare() {

}

func (d *Drop) Execute() []*db.Row {
	return nil
}
