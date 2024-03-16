package db

type Row struct {
	Data map[string]interface{}
}

func NewRow(data map[string]interface{}) *Row {
	return &Row{
		Data: data,
	}
}
