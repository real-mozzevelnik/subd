package btree

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

func (c Comparator) compare(itemValue interface{}) bool {
	value := itemValue
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
		case int:
			if value.(int) < c.Value.(int) {
				return true
			}
		case float64:
			if value.(float64) < c.Value.(float64) {
				return true
			}
		}

	case "le":
		switch value.(type) {
		case int:
			if value.(int) <= c.Value.(int) {
				return true
			}
		case float64:
			if value.(float64) <= c.Value.(float64) {
				return true
			}
		}

	case "gt":
		switch value.(type) {
		case int:
			if value.(int) > c.Value.(int) {
				return true
			}
		case float64:
			if value.(float64) > c.Value.(float64) {
				return true
			}
		}

	case "ge":
		switch value.(type) {
		case int:
			if value.(int) >= c.Value.(int) {
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
