package dml

import (
	"fmt"
	"regexp"
	"strings"
	"subd/internal/db"
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

func (u *Update) Prepare() (err error) {
	var value interface{}
	req := regexp.MustCompile(`[\s\(]*(?i)SET\s+`).Split(u.request, -1)
	if len(req) == 1 {
		err := fmt.Errorf("invalid update request: %s", u.request)
		panic(err)
	}
	u.tableName = strings.Replace(req[0], " ", "", -1)

	req = regexp.MustCompile(`[\s\)]+(?i)WHERE\s+`).Split(req[1], -1)
	schema := u.dataBase.GetTableSchema(u.tableName)

	if len(req) == 2 {
		whereExpr := strings.Split(req[1], " ")
		cmp, err := utils.NewComparatorByWhereExpr(whereExpr, schema)

		if err != nil {
			return err
		}

		u.comparators = append(u.comparators, cmp)
		fmt.Println(u.comparators[0].FieldName, u.comparators[0].Operation, u.comparators[0].Value)
	}

	rawSetExpr := strings.Split(req[0], ",")
	re := regexp.MustCompile(`\s*=\s*`)
	for _, expr := range rawSetExpr {
		rawData := re.Split(expr, -1)

		if value, err = utils.TypeValidation(rawData[1], schema[rawData[0]]); err != nil {
			return err
		}

		u.data[rawData[1]] = value
	}
	return err
}

func (d *Update) Execute() (resultSet []map[string]interface{}, err error) {
	switch len(d.comparators) {
	case 0:
		d.dataBase.Update(d.tableName, d.data)
	default:
		d.dataBase.UpdateWhere(d.tableName, d.data, d.comparators)
	}
	return resultSet, err
}
