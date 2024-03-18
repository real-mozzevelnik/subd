package dml

import (
	"strings"
	"subd/internal/db"
	"subd/internal/parser/statement/dql"
)

type Delete struct {
	dataBase    *db.DB
	request     string
	tableName   string
	comparators []db.Comparator
}

func NewDelete(db *db.DB, req string) *Delete {
	return &Delete{
		dataBase: db,
		request:  req,
	}
}

func (d *Delete) Prepare() {
	words := strings.Fields(d.request)
	wordsLen := len(words)

	for index := 0; index < wordsLen-1; index++ {
		if strings.ToLower(words[index]) == "delete" {
			index += 2
			d.tableName = words[index]
			continue
		}
		if strings.ToLower(words[index]) == "where" {
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

				if (index < wordsLen-1) && (strings.ToLower(words[index]) == "and") {
					continue
				}
				break
			}
		}
	}
}

// REFACTOR: refactor return value
func (d *Delete) Execute() []*db.Row {
	if len(d.comparators) == 0 {
		d.dataBase.Delete(d.tableName)

	} else {
		d.dataBase.DeleteWhere(d.tableName, d.comparators)
	}

	return nil
}
