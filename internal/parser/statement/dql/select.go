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
	regexpSelect = regexp.MustCompile(`(?s)(.*)\s(?i)FROM\s+([^\s]*)\s*?(?:\s+(?i)WHERE\s+((?s).*))?`)
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
	// clear from trash
	s.request = strings.NewReplacer("\t", " ", "\n", " ").Replace(s.request)

	// used regular expression
	match := regexpSelect.FindStringSubmatch(s.request)

	s.tableName = strings.Replace(match[2], " ", "", -1)
	s.searchedFields = utils.SplitTrim(match[1], ",", " ", "\t", "\n", "(", ")")

	// get table schema by table name
	tableSchema := s.dataBase.GetTableSchema(s.tableName)
	for _, field := range s.searchedFields {
		// checking whethet a field exists in the table
		_, ok := tableSchema[field]

		if !ok {
			// special case
			if field == "*" {
				continue
			} else {

				return &errors.Error{
					Msg:  "Unknown field: " + field,
					Code: errors.NOT_FOUND_DATA,
					Req:  s.request,
				}
			}
		}
	}

	if match[3] != "" {
		s.comparators, err = utils.ProcessWhereExpr(match[3], tableSchema)
		if err != nil {
			err.Req = "select " + s.request
			return err
		}
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
