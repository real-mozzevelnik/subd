package dml

import (
	"regexp"
	"strings"
	"subd/internal/db"
	"subd/internal/parser/errors"
	"subd/internal/utils"
)

type Delete struct {
	dataBase    *db.DB
	request     string
	tableName   string
	comparators []utils.Comparator
}

func NewDelete(db *db.DB, req string) *Delete {
	return &Delete{
		dataBase:    db,
		request:     req,
		comparators: make([]utils.Comparator, 0),
	}
}

func (d *Delete) Prepare() (err *errors.Error) {
	spaceReplacer := strings.NewReplacer(" ", "")

	re := regexp.MustCompile(`\s+(?i)where\s+`)
	splitByWhere := re.Split(d.request, -1)

	switch len(splitByWhere) {
	case 1:
		{
			d.tableName = spaceReplacer.Replace(splitByWhere[0])
		}
	case 2:
		{
			d.tableName = spaceReplacer.Replace(splitByWhere[0])
			d.comparators, err = utils.ProcessWhereExpr(splitByWhere[1], d.dataBase.GetTableSchema(d.tableName))
		}
	default:
		{
			// TODO:z
			err = &errors.Error{Msg: "len > 2"}
		}
	}

	if err != nil {
		err.Req = "delete from" + d.request
		return err
	}

	return nil
}

func (d *Delete) Execute() (resultSet []map[string]interface{}, err *errors.Error) {
	switch len(d.comparators) {
	case 0:
		d.dataBase.Delete(d.tableName)
	default:
		d.dataBase.DeleteWhere(d.tableName, d.comparators)
	}
	return nil, nil
}
