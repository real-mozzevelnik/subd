package main

import (
	"encoding/json"
	"strings"
	"subd/internal/db"
	"subd/internal/parser/parser"
	"subd/internal/socket_server"
)

var database *db.DB = nil

func main() {
	database = db.NewDB()

	socket_server.Handle("sql_statement", SqlStatementHandler)
	socket_server.Handle("db_info", DBInfoHandler)
	socket_server.ListenAndServe(":8090")
}

func SqlStatementHandler(data *json.RawMessage) (map[string]interface{}, error) {
	var d map[string]interface{}
	err := json.Unmarshal(*data, &d)
	if err != nil {
		return nil, err
	}

	sql_statement := d["sql_statement"]
	sql_statement = strings.ReplaceAll(sql_statement.(string), "\n", "")
	requestParser := parser.New(database)
	requestParser.Accept(sql_statement.(string))
	resultSet, error := requestParser.Execute()
	if error != nil {
		return nil, error
	}

	response := make(map[string]interface{})
	response["result"] = resultSet

	return response, nil
}

func DBInfoHandler(data *json.RawMessage) (map[string]interface{}, error) {
	return map[string]interface{}{}, nil
}
