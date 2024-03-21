package utils

// operations:
// "eq" ==
// "neq" !=
// "lt" <
// "le" <=
// "gt" >
// "ge" >=
type Comparator struct {
	FieldName string
	Value     interface{}
	Operation string
}

func NewComparator(fieldName string, value interface{}, operation string) Comparator {
	return Comparator{
		FieldName: fieldName,
		Value:     value,
		Operation: operation,
	}
}

func (c Comparator) Compare(value interface{}) bool {
	switch c.Operation {
	case "eq":
		if value == c.Value {
			return true
		}

	case "neq":
		if value != c.Value {
			return true
		}

	case "lt":
		switch value.(type) {
		case int64:
			if value.(int64) < c.Value.(int64) {
				return true
			}
		case float64:
			if value.(float64) < c.Value.(float64) {
				return true
			}
		}

	case "le":
		switch value.(type) {
		case int64:
			if value.(int64) <= c.Value.(int64) {
				return true
			}
		case float64:
			if value.(float64) <= c.Value.(float64) {
				return true
			}
		}

	case "gt":
		switch value.(type) {
		case int64:
			if value.(int64) > c.Value.(int64) {
				return true
			}
		case float64:
			if value.(float64) > c.Value.(float64) {
				return true
			}
		}

	case "ge":
		switch value.(type) {
		case int64:
			if value.(int64) >= c.Value.(int64) {
				return true
			}
		case float64:
			if value.(float64) >= c.Value.(float64) {
				return true
			}
		}
	}
	return false
}
