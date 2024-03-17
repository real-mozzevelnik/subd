package db

type Row struct {
	Data map[string]interface{}
}

func newRow(data map[string]interface{}) *Row {
	return &Row{
		Data: data,
	}
}
