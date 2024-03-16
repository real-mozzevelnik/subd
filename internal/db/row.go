package db

type row struct {
	Data map[string]interface{}
}

func newRow(data map[string]interface{}) *row {
	return &row{
		Data: data,
	}
}
