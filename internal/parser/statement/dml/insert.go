package dml

import (
	"regexp"
	"strings"
	"subd/internal/db"
	"subd/internal/utils"
)

type Insert struct {
	dataBase  *db.DB
	request   string
	tableName string
	data      map[string]interface{}
}

func NewInsert(db *db.DB, req string) *Insert {
	return &Insert{
		dataBase: db,
		request:  req,
		data:     make(map[string]interface{}),
	}
}

func (i *Insert) Prepare() (err error) {
	re := regexp.MustCompile(`(\w+)[\s\(]+(.*?)[\)\s]\s*(?i)VALUES[\s\(]+(.*)[\)\s]$`)
	match := re.FindStringSubmatch(i.request)

	i.tableName = match[1]
	fields := strings.Split(match[2], ",")
	values := strings.Split(match[3], ",")

	i.data, err = utils.FillTheData(fields, values, i.dataBase.GetTableSchema(i.tableName))
	return err
}

func (i *Insert) Execute() (resultSet []map[string]interface{}, err error) {
	i.dataBase.Insert(i.tableName, i.data)
	return resultSet, err
}

// stableReplacer := strings.NewReplacer("(", "", ")", "")
// re := regexp.MustCompile(`\s*(?i)VALUES\s*`)

// splitByValues := re.Split(i.request, -1)

// re = regexp.MustCompile(`([^( ]*)\s*(.*)`)
// rawTableNameAndFields := re.FindStringSubmatch(splitByValues[0])

// i.tableName = rawTableNameAndFields[1]

// rawTableNameAndFields[2] = stableReplacer.Replace(rawTableNameAndFields[2])
// fields := strings.Split(rawTableNameAndFields[2], ",")

// rawValues := stableReplacer.Replace(splitByValues[1])
// values := strings.Split(rawValues, ",")

// i.data, err = utils.FillTheData(fields, values, i.dataBase.GetTableSchema(i.tableName))
