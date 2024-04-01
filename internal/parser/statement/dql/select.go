package dql

import (
	"regexp"
	"strings"
	"subd/internal/db"
	"subd/internal/parser/errors"
	"subd/internal/utils"
)

var (
	OperatorsMap = map[string]string{
		"==": "eq",
		"!=": "neq",
		"<":  "lt",
		"<=": "le",
		">":  "gt",
		">=": "ge",
	}
	regexpSelect = regexp.MustCompile(`(?s)(.*)\s(?i)FROM\s+([^\s]*)\s*?(?:\s+(?i)WHERE\s+(?s)(.*))?`)
)

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
	// fmt.Println("req:", s.request)
	s.request = strings.NewReplacer("\t", " ", "\n", " ").Replace(s.request)

	match := regexpSelect.FindStringSubmatch(s.request)

	// for idx, v := range match {
	// 	fmt.Println("match:", idx, v)
	// }

	// REFACTOR: check if exist table name
	s.tableName = strings.Replace(match[2], " ", "", -1)
	tableSchema := s.dataBase.GetTableSchema(s.tableName)

	s.searchedFields = utils.SplitTrim(match[1], ",", " ", "\t", "\n", "(", ")")

	for _, field := range s.searchedFields {
		_, ok := tableSchema[field]
		if !ok {
			return &errors.Error{
				Msg:  "Unknown field: " + field,
				Code: errors.NOT_FOUND_DATA,
				Req:  s.request,
			}
		}
	}

	// fmt.Println("table name:", s.tableName)
	// fmt.Println("searched fields:", s.searchedFields)

	if match[3] != "" {
		condition := utils.FieldsN(match[3], 3)

		condition[0] = strings.TrimPrefix(condition[0], "(")
		condition[2] = strings.TrimSuffix(condition[2], ")")

		cmp, err := utils.NewComparatorByWhereExpr(condition, tableSchema)

		if err != nil {
			return &errors.Error{
				Msg:  err.Error(),
				Code: errors.INVALID_REQUEST,
				Req:  s.request,
			}
		}

		s.comparators = append(s.comparators, cmp)
		// fmt.Println("comparators:", s.comparators)
	}
	// fmt.Println()
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
