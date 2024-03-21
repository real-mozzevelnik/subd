package dml

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"subd/internal/db"
	"subd/internal/parser/statement/dql"
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

func (u *Update) Prepare() {
	re := regexp.MustCompile(`[\s\(]*(?i)SET\s+`)
	req := re.Split(u.request, -1)

	if len(req) == 1 {
		err := fmt.Errorf("invalid update request: %s", u.request)
		panic(err)
	}

	u.tableName = strings.Replace(req[0], " ", "", -1)
	fmt.Println(u.tableName)

	re = regexp.MustCompile(`[\s\)]+(?i)WHERE\s+`)
	req = re.Split(req[1], -1)

	if len(req) == 2 {
		whereExpr := strings.Split(req[1], " ")
		u.newWhereComparator(whereExpr)
	}

	var err error
	schema := u.dataBase.GetTableSchema(u.tableName)

	rawSetExpr := strings.Split(req[0], ",")
	for _, expr := range rawSetExpr {
		re = regexp.MustCompile(`\s*=\s*`)
		rawData := re.Split(expr, -1)

		if len(rawData) != 2 {
			err = fmt.Errorf("invalid update set expression: %s\nrequest: %s", expr, u.request)
		} else {

			// rawData[0] - field
			switch schema[rawData[0]] {
			case "TEXT":
				{
					// rawData[1] - field value
					if rawData[1][0] != '\'' && rawData[1][0] != '"' {
						err = fmt.Errorf("invalid value type in field: %v\nrequest: %s", rawData, u.request)
						break
					}
					u.data[rawData[0]] = rawData[1]
				}
			case "INTEGER":
				{
					if rawData[1][0] == '\'' || rawData[1][0] == '"' {
						err = fmt.Errorf("invalid value type in field: %v\nrequest: %s", rawData, u.request)
						break
					}
					u.data[rawData[0]], err = strconv.Atoi(rawData[1])
				}
			default:
				err = fmt.Errorf("unknown value type in field: %s\nrequest: %s", rawData, u.request)
			}
		}

		if err != nil {
			panic(err)
		}
	}
}

func (u *Update) newWhereComparator(whereExpr []string) {
	schema := u.dataBase.GetTableSchema(u.tableName)
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
	u.comparators = append(u.comparators, utils.NewComparator(whereExpr[0], value, dql.OperatorsMap[whereExpr[1]]))

	fmt.Printf("UPDATE %s TABLE, values: <%v> typeof<%v>\n", u.tableName, value, reflect.TypeOf(value))
	if len(u.comparators) > 0 {
		for idx, c := range u.comparators {
			fmt.Printf("comparator%d: %v\n", idx, c)
		}
	}
}

func (d *Update) Execute() []map[string]interface{} {

	return nil
}
