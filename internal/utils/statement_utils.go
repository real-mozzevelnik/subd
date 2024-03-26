package utils

import (
	"fmt"
	"strconv"
	"strings"
)

var OperatorsMap = map[string]string{
	"==": "eq",
	"!=": "neq",
	"<":  "lt",
	"<=": "le",
	">":  "gt",
	">=": "ge",
}

// trim spaces and split by ',' sym
func SplitTrim(str, sep string, cutsets ...string) []string {
	for _, cutset := range cutsets {
		str = strings.Replace(str, cutset, "", -1)
	}
	return strings.Split(str, sep)
}

// checks entered data for correctness of types and returned the map with them
func FillTheData(fields []string, values []string, tableSchema map[string]interface{}) (filledData map[string]interface{}, err error) {
	filledData = make(map[string]interface{})

	for idx, field := range fields {
		filledData[field], err = TypeValidation(values[idx], tableSchema[field])
		if err != nil {
			return nil, err
		}
	}

	return filledData, nil
}

func TypeValidation(value string, typeValue interface{}) (convertedValue interface{}, err error) {
	switch typeValue {
	case "INTEGER":
		convertedValue, err = strconv.ParseInt(value, 10, 0)
		if err != nil {
			return nil, fmt.Errorf("invalid type for <%s>. Field has INTEGER type, but value isn't", value)
		}

	case "TEXT":
		if value[0] == '\'' && value[len(value)-1] == '\'' {
			convertedValue = strings.Replace(value, "'", "", -1)
			break
		}

		if value[0] == '"' && value[len(value)-1] == '"' {
			convertedValue = strings.Replace(value, "\"", "", -1)
			break
		}
		return nil, fmt.Errorf("invalid type for <%s>.\nField has TEXT type, but value isn't", value)

	case "BOOL":
		return nil, fmt.Errorf("bool type isn't supported")

	case "FLOAT":
		return nil, fmt.Errorf("float type isn't supported")

	default:
		return nil, fmt.Errorf("unknown type field for <%s>", value)

	}

	return convertedValue, nil
}

func NewComparatorByWhereExpr(whereExpr []string, tableSchema map[string]interface{}) (cmp Comparator, err error) {
	if len(whereExpr) != 3 {
		return Comparator{}, fmt.Errorf("invalid where expression: %s", whereExpr)
	}

	var value interface{}
	switch tableSchema[whereExpr[0]] {
	case "INTEGER":
		value, err = strconv.ParseInt(whereExpr[2], 10, 0)
		if err != nil {
			return Comparator{}, fmt.Errorf("invalid type in where expression: %s\nField has INTEGER type, but value isn't", whereExpr)
		}

	case "TEXT":
		switch whereExpr[1] {
		case "==", "!=":
			if whereExpr[2][0] == '\'' && whereExpr[2][len(whereExpr[2])-1] == '\'' {
				value = strings.Replace(whereExpr[2], "'", "", -1)
				break
			}

			if whereExpr[2][0] == '"' && whereExpr[2][len(whereExpr[2])-1] == '"' {
				value = strings.Replace(whereExpr[2], "\"", "", -1)
				break
			}
			return Comparator{}, fmt.Errorf("invalid type in where expression: %s.\nField has TEXT type, but value isn't", whereExpr)

		default:
			return Comparator{}, fmt.Errorf("invalid operator in where expression: %s, TEXT can't be compared using it", whereExpr[1])
		}

	case "FLOAT":
		// logic to float

	case "BOOL":
		// Logic to bool

	default:
		return Comparator{}, fmt.Errorf("unknown type field in where expression: %s can't supported", whereExpr[1])
	}

	return NewComparator(whereExpr[0], value, OperatorsMap[whereExpr[1]]), nil
}
