package dql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"subd/internal/db"
	"subd/internal/utils"
)

var OperatorsMap = map[string]string{
	"==": "eq",
	"!=": "neq",
	"<":  "lt",
	"<=": "le",
	">":  "gt",
	">=": "ge",
}

type Select struct {
	dataBase       *db.DB
	request        string
	tableName      string
	searchedFields []string
	comparators    []utils.Comparator
}

func NewSelect(db *db.DB, req string) *Select {
	return &Select{
		dataBase:    db,
		request:     req,
		comparators: make([]utils.Comparator, 0),
	}
}

func (s *Select) Prepare() {
	re := regexp.MustCompile(`([^ ]*)\s+(?i)FROM\s+([^ ]*)(?:\s+(?i)WHERE\s+(.*))?`)
	match := re.FindStringSubmatch(s.request)

	if len(match) < 3 {
		err := fmt.Errorf("invalid select request: %s", s.request)
		panic(err)
	}

	s.comparators = s.comparators[:0]
	replacer := strings.NewReplacer("(", "", ")", "")

	rawColumnNames := match[1]
	s.searchedFields = strings.Split(replacer.Replace(rawColumnNames), ",")

	s.tableName = match[2]

	if match[3] != "" {
		rawWhereExpr := match[3]
		whereExpr := strings.Split(rawWhereExpr, " ")
		s.newWhereComparator(whereExpr)
	}
}

func (s *Select) newWhereComparator(whereExpr []string) {
	schema := s.dataBase.GetTableSchema(s.tableName)
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
	s.comparators = append(s.comparators, utils.NewComparator(whereExpr[0], value, OperatorsMap[whereExpr[1]]))

	// fmt.Printf("from %s, Select fields: %s, values: <%v> typeof<%v>\n", s.tableName, s.searchedFields, value, reflect.TypeOf(value))
	// if len(s.comparators) > 0 {
	// 	for idx, c := range s.comparators {
	// 		fmt.Printf("comparator%d: %v\n", idx, c)
	// 	}
	// }
}

func (s *Select) Execute() []map[string]interface{} {
	if len(s.comparators) > 0 {
		return s.dataBase.SelectWhere(s.tableName, s.comparators, s.searchedFields)
	}
	return s.dataBase.Select(s.tableName, s.searchedFields)
}
