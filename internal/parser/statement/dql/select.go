package dql

import (
	"regexp"
	"strings"
	"subd/internal/db"
	"subd/internal/parser/errors"
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

func (s *Select) Prepare() (err *errors.Error) {
	match := regexp.MustCompile(`(.*)\s(?i)FROM\s+([^ ]*)(?:\s+(?i)WHERE\s+(.*))?`).FindStringSubmatch(s.request)

	s.searchedFields = utils.SplitTrim(match[1], ",", " ", "(", ")")
	s.tableName = match[2]

	if match[3] != "" {
		cmp, err := utils.NewComparatorByWhereExpr(strings.Split(match[3], " "), s.dataBase.GetTableSchema(s.tableName))

		if err != nil {
			return &errors.Error{
				Msg:  err.Error(),
				Code: errors.INVALID_REQUEST,
				Req:  s.request,
			}
		}

		s.comparators = append(s.comparators, cmp)
	}
	return nil
}

func (s *Select) Execute() (resSet []map[string]interface{}, err *errors.Error) {
	switch len(s.comparators) {
	case 0:
		return s.dataBase.Select(s.tableName, s.searchedFields), nil
	default:
		return s.dataBase.SelectWhere(s.tableName, s.comparators, s.searchedFields), nil
	}
}
