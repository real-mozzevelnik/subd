package dml

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"subd/internal/db"
	"subd/internal/parser/statement/dql"
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
		dataBase: db,
		request:  req,
	}
}

func (d *Delete) Prepare() {
	var err error
	req := strings.Split(d.request, "WHERE")

	switch len(req) {
	case 1:
		{
			d.tableName = strings.NewReplacer(" ", "").Replace(req[0])
		}
	case 2:
		{
			re := regexp.MustCompile(`\s*(\w+)\s+(?i)WHERE\s+(.*)`)
			match := re.FindStringSubmatch(d.request)

			fmt.Println(len(match))
			if len(match) != 3 {
				err = fmt.Errorf("invalid delete request: %s", d.request)
				break
			}

			d.tableName = match[1]
			whereExpr := strings.Split(match[2], " ")
			d.newWhereComparator(whereExpr)
		}
	default:
		err = fmt.Errorf("invalid delete request: %s", d.request)
	}

	if err != nil {
		panic(err)
	}
}

// REFACTOR: refactor return value
func (d *Delete) Execute() []map[string]interface{} {
	if len(d.comparators) == 0 {
		d.dataBase.Delete(d.tableName)
	} else {
		d.dataBase.DeleteWhere(d.tableName, d.comparators)
	}
	return nil
}

func (d *Delete) newWhereComparator(whereExpr []string) {
	schema := d.dataBase.GetTableSchema(d.tableName)
	replacer := strings.NewReplacer("'", "", "\"", "")

	// value for 2 parameter comparator
	var value interface{}
	var err error

	// switch by typeof field
	switch schema[whereExpr[0]] {
	case "TEXT":
		{
			//whereExpr[0] - field, [1] - operator, [2] - value
			if whereExpr[1] == ">=" || whereExpr[1] == "<=" || whereExpr[1] == "<" || whereExpr[1] == ">" {
				err = fmt.Errorf("\"%s\" operator isn't available for TEXT comparision", whereExpr[1])
				break
			}

			if whereExpr[2][0] == '\'' || whereExpr[2][0] == '"' {
				whereExpr[2] = replacer.Replace(whereExpr[2])
				//no need to convert
				value = whereExpr[2]
			} else {
				err = fmt.Errorf("comparision field <%s> typeof(TEXT) with no string: <%s>", whereExpr[0], whereExpr[2])
				break
			}
		}
	case "INTEGER":
		{
			if whereExpr[2][0] == '"' || whereExpr[2][0] == '\'' {
				err = fmt.Errorf("comprasion field <%s> typeof(INTEGER) with TEXT: <%s>", whereExpr[0], whereExpr[2])
			}
			value, _ = strconv.ParseInt(whereExpr[2], 10, 0)
		}
	}

	if err != nil {
		panic(err)
	}

	// adding comparator
	d.comparators = append(d.comparators, utils.NewComparator(whereExpr[0], value, dql.OperatorsMap[whereExpr[1]]))
}
