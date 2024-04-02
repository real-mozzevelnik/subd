package dml

import (
	"fmt"
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

func (d *Delete) Prepare() *errors.Error {
	spaceReplacer := strings.NewReplacer(" ", "")

	re := regexp.MustCompile(`\s+(?i)where\s+`)
	splitByWhere := re.Split(d.request, -1)

	fmt.Println("d.request:", d.request)
	fmt.Println(splitByWhere)

	var err error
	switch len(splitByWhere) {
	case 1:
		{
			d.tableName = spaceReplacer.Replace(splitByWhere[0])
		}
	case 2:
		{
			d.tableName = spaceReplacer.Replace(splitByWhere[0])
			whereExpr := strings.Fields(splitByWhere[1])
			cmp, err := utils.NewComparatorByWhereExpr(whereExpr, d.dataBase.GetTableSchema(d.tableName))

			if err != nil {
				break
			}

			d.comparators = append(d.comparators, cmp)
		}
	default:
		{
			err = fmt.Errorf("invalid request")
		}
	}

	if err != nil {
		return &errors.Error{
			Msg:  err.Error(),
			Code: errors.INVALID_REQUEST,
			Req:  d.request,
		}
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
