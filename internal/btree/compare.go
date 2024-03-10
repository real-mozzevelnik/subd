package btree

// operations:
// "eq" =
// "lt" <
// "le" <=
// "gt" >
// "ge" >=
type Comparator struct {
	FieldName string
	Value     interface{}
	Operation string
}

func (c Comparator) compare(item *Item) bool {
	value := item.value.(map[string]interface{})[c.FieldName]

	switch c.Operation {
	case "eq":
		if value == c.Value {
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
