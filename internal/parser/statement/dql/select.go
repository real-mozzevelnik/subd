package dql

import (
	"fmt"
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
	DataBase    *db.DB
	Request     string
	tableName   string
	columnName  string
	comparators []db.Comparator
}

func (s *Select) Prepare() {
	words := strings.Fields(s.Request)
	wordsLen := len(words)

	for index := 0; index < wordsLen-1; index++ {
		if words[index] == "SELECT" {
			index++
			s.columnName = words[index]
			continue
		}
		// Dagestan
		if words[index] == "FROM" {
			index++
			s.tableName = words[index]
			continue
		}
		// TODO: Correct type conversion666, adding keyword OR
		// REFACTOR: idk, just doesnt like how it looks
		if words[index] == "WHERE" {
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

				if (index < wordsLen-1) && (words[index] == "AND") {
					continue
				}
				break
			}
			continue
		}
	}
}

// for logs
var selectId = 0

// TODO: Return db.Result
func (s *Select) Execute() {
	var data []*db.Result

	if len(s.comparators) == 0 {
		data = s.DataBase.Select(s.tableName)
	} else {
		data = s.DataBase.SelectWhere(s.tableName, s.comparators)
	}

	fmt.Printf("SELECT NUMBER: %d\n", selectId)
	selectId++
	for _, d := range data {
		fmt.Printf("%v\n", d)
	}
	fmt.Println()
}
