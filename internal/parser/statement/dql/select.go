package dql

import (
	"fmt"
	"regexp"
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

func (s *Select) Prepare() (err error) {
	s.comparators = s.comparators[:0]
	replacer := strings.NewReplacer("(", "", ")", "")
	match := regexp.MustCompile(`([^ ]*)\s+(?i)FROM\s+([^ ]*)(?:\s+(?i)WHERE\s+(.*))?`).FindStringSubmatch(s.request)

	rawColumnNames := match[1]
	s.searchedFields = strings.Split(replacer.Replace(rawColumnNames), ",")

	s.tableName = match[2]

	if match[3] != "" {
		rawWhereExpr := match[3]
		whereExpr := strings.Split(rawWhereExpr, " ")
		cmp, err := utils.NewComparatorByWhereExpr(whereExpr, s.dataBase.GetTableSchema(s.tableName))

		if err != nil {
			return fmt.Errorf("%s.\nOriginal Request: %s", err, s.request)
		}

		s.comparators = append(s.comparators, cmp)
	}

	return err
}

func (s *Select) Execute() (resultSet []map[string]interface{}, err error) {

	switch len(s.comparators) {
	case 0:
		return s.dataBase.Select(s.tableName, s.searchedFields), nil
	default:
		return s.dataBase.SelectWhere(s.tableName, s.comparators, s.searchedFields), nil
	}
}
