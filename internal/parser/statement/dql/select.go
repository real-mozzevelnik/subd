package dql

import (
	"strings"
	"subd/internal/db"
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
	dataBase    *db.DB
	request     string
	tableName   string
	columnName  string
	comparators []db.Comparator
}

func NewSelect(db *db.DB, req string) *Select {
	return &Select{
		dataBase: db,
		request:  req,
	}
}

func (s *Select) Prepare() {
	words := strings.Fields(s.request)
	wordsLen := len(words)

	for index := 0; index < wordsLen-1; index++ {
		if strings.ToLower(words[index]) == "select" {
			index++
			s.columnName = words[index]
			continue
		}
		// Dagestan
		if strings.ToLower(words[index]) == "from" {
			index++
			s.tableName = words[index]
			continue
		}

		// TODO: Correct type conversion666, adding keyword OR
		// REFACTOR: idk, just doesnt like how it looks
		if strings.ToLower(words[index]) == "where" {
			for {
				if index >= wordsLen-1 {
					break
				}

				comparator := db.NewComparator(
					words[index+1],
					words[index+3],
					OperatorsMap[words[index+2]],
				)

				s.comparators = append(s.comparators, comparator)
				index += 4

				if (index < wordsLen-1) && (strings.ToLower(words[index]) == "and") {
					continue
				}
				break
			}
			continue
		}
	}
}

func (s *Select) Execute() []*db.Row {
	if len(s.comparators) == 0 {
		return s.dataBase.Select(s.tableName)
	} else {
		return s.dataBase.SelectWhere(s.tableName, s.comparators)
	}
}
