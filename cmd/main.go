package main

import (
	"encoding/json"
	"strconv"
	"subd/internal/socket_server"
)

func main() {
	socket_server.Handle("sql_statement", SqlStatementHandler)
	socket_server.ListenAndServe(":8090")
}

func SqlStatementHandler(data *json.RawMessage) (map[string]interface{}, error) {
	resp := make(map[string]interface{})
	for i := 0; i < 200000; i++ {
		resp[strconv.Itoa(i)] = map[string]interface{}{
			"testField1": i,
			"testField2": strconv.Itoa(i),
		}
	}

	return map[string]interface{}{
		"test": resp,
	}, nil
}
