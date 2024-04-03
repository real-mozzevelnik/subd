package dml

import (
	"regexp"
	"strings"
	"subd/internal/db"
	"subd/internal/parser/errors"
	"subd/internal/utils"
)

type Update struct {
	dataBase    *db.DB
	request     string
	tableName   string
	comparators []utils.Comparator
	data        map[string]interface{}
}

func NewUpdate(db *db.DB, req string) *Update {
	return &Update{
		dataBase: db,
		request:  req,
		data:     make(map[string]interface{}, 0),
	}
}

func (u *Update) Prepare() *errors.Error {
	req := regexp.MustCompile(`[\s\(]*(?i)SET\s+`).Split(u.request, -1)

	u.tableName = strings.Replace(req[0], " ", "", -1)

	req = regexp.MustCompile(`[\s\)]+(?i)WHERE\s+`).Split(req[1], -1)
	schema := u.dataBase.GetTableSchema(u.tableName)

	if len(req) == 2 {
		whereExpr := utils.SplitTrim(req[1], " ", "\t", "\n")

		// fmt.Println("expr")
		// for idx, e := range whereExpr {
		// 	fmt.Printf("%d: _%s_\n", idx, e)
		// }

		cmp, err := utils.NewComparatorByWhereExpr(whereExpr, schema)

		if err != nil {
			return &errors.Error{
				Msg:  err.Error(),
				Code: errors.INVALID_REQUEST,
				Req:  "update " + u.request,
			}
		}

		u.comparators = append(u.comparators, cmp)
	}

	rawSetExpr := strings.Split(req[0], ",")
	for _, expr := range rawSetExpr {
		rawData := utils.SplitTrim(expr, "=", " ", "\t", "\n")
		value, err := utils.TypeValidation(rawData[1], schema[rawData[0]])

		if err != nil {
			return &errors.Error{
				Msg:  err.Error(),
				Code: errors.INVALID_REQUEST,
				Req:  u.request,
			}
		}

		u.data[rawData[0]] = value
	}

	return nil
}

func (d *Update) Execute() (resultSet []map[string]interface{}, err *errors.Error) {
	switch len(d.comparators) {
	case 0:
		d.dataBase.Update(d.tableName, d.data)
	default:
		d.dataBase.UpdateWhere(d.tableName, d.data, d.comparators)
	}
	return resultSet, nil
}
