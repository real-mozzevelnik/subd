package dml

import (
	"regexp"
	"subd/internal/db"
	"subd/internal/parser/errors"
	"subd/internal/utils"
)

var (
	re = regexp.MustCompile(`(\w+)\s*?\(((?s).*?)\)\s*?(?i)VALUES\s*?\(((?s).*)\)$`)
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

func (i *Insert) Prepare() *errors.Error {
	match := re.FindStringSubmatch(i.request)

	if len(match) != 4 {
		return &errors.Error{
			Msg:  "unknown insert request syntax",
			Code: errors.INVALID_REQUEST,
			Req:  "insert into " + i.request,
		}
	}

	// TODO: adding checking whether a table exist with this name
	i.tableName = match[1]

	fields := utils.SplitTrim(match[2], ",", " ", "\t", "\n")
	values := utils.SplitTrim(match[3], ",", " ", "\t", "\n")

	if len(fields) != len(values) {
		return &errors.Error{
			Msg:  "Number of fields isn't equal to the numbere of values",
			Code: errors.INVALID_REQUEST,
			Req:  "insert into " + i.request,
		}
	}

	var err error
	i.data, err = utils.FillTheData(fields, values, i.dataBase.GetTableSchema(i.tableName))

	if err != nil {
		return &errors.Error{
			Msg:  err.Error(),
			Code: errors.INVALID_REQUEST,
			Req:  "INSERT INTO" + i.request,
		}
	}

	return nil
}

func (i *Insert) Execute() (resultSet []map[string]interface{}, err *errors.Error) {
	i.dataBase.Insert(i.tableName, i.data)
	return resultSet, nil
}
