package dml

import (
	"strings"
	"subd/internal/db"
	"subd/internal/parser/statement/dql"
)

type Delete struct {
	DataBase    *db.DB
	Request     string
	tableName   string
	comparators []db.Comparator
}

func (d *Delete) Prepare() {
	words := strings.Fields(d.Request)
	wordsLen := len(words)

	for index := 0; index < wordsLen-1; index++ {
		if words[index] == "DELETE" {
			index += 2
			d.tableName = words[index]
			continue
		}
		if words[index] == "WHERE" {
			for {
				if index >= wordsLen-1 {
					break
				}

				comparator := db.NewComparator(
					words[index+1],
					words[index+3],
					dql.OperatorsMap[words[index+2]],
				)

				d.comparators = append(d.comparators, comparator)
				index += 4

				if (index < wordsLen-1) && (words[index] == "AND") {
					continue
				}
				break
			}
		}
	}
}

func (d *Delete) Execute() {
	if len(d.comparators) == 0 {
		d.DataBase.Delete(d.tableName)

	} else {
		d.DataBase.DeleteWhere(d.tableName, d.comparators)
	}
}
