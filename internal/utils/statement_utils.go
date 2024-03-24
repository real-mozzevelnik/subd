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
		{
			convertedValue, err = strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("int isn't text")
			}
		}
	case "TEXT":
		{
			if value[0] == '\'' && value[len(value)-1] == '\'' {
				convertedValue = strings.Replace(value, "'", "", -1)
				break
			}

			if value[0] == '"' && value[len(value)-1] == '"' {
				convertedValue = strings.Replace(value, "\"", "", -1)
				break
			}

			return nil, fmt.Errorf("invalid type %s.\nField has TEXT type, but value isn't", value)

		}
	case "BOOL":
		{
			return nil, fmt.Errorf("bool type isn't supported")
		}
	case "FLOAT":
		{
			return nil, fmt.Errorf("float type isn't supported")
		}
	default:
		{
			return nil, fmt.Errorf("unknown type field")
		}
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
		{
			value, err = strconv.Atoi(whereExpr[2])
			if err != nil {
				return Comparator{}, fmt.Errorf("invalid type in where expression: %s\nField has INTEGER type, but value isn't", whereExpr)
			}
		}
	case "TEXT":
		{
			switch whereExpr[1] {
			case "==", "!=":
				{
					if whereExpr[2][0] == '\'' && whereExpr[2][len(whereExpr)-1] == '\'' {
						value = strings.Replace(whereExpr[2], "'", "", -1)
						break
					}

					if whereExpr[2][0] == '"' && whereExpr[2][len(whereExpr)-1] == '"' {
						value = strings.Replace(whereExpr[2], "\"", "", -1)
						break
					}

					return Comparator{}, fmt.Errorf("invalid type in where expression: %s.\nField has TEXT type, but value isn't", whereExpr)
				}
			default:
				{
					return Comparator{}, fmt.Errorf("invalid operator in where expression: %s, TEXT can't be compared using it", whereExpr[1])
				}
			}
		}
	case "FLOAT":
		{
			// logic to float
		}
	case "BOOL":
		{
			// Logic to bool
		}
	default:
		{
			return Comparator{}, fmt.Errorf("unknown type field in where expression: %s can't supported", whereExpr[1])
		}
	}

	return NewComparator(whereExpr[0], value, OperatorsMap[whereExpr[1]]), nil
}
