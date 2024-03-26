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

// SplitTrim trim cutsets characters and split by sep string
// if sep is space, returns slice length one with an origin string after trimming
func SplitTrim(str, sep string, cutsets ...string) []string {
	for _, cutset := range cutsets {
		str = strings.Replace(str, cutset, "", -1)
	}
	if sep != "" {
		return strings.Split(str, sep)
	} else {
		return append(make([]string, 1), str)
	}
}

// FieldsN splits the string str around each instance of one or more space characters,
// count elements in slice controls by second param elemNum
func FieldsN(str string, elemNum int) []string {
	if str == "" {
		return nil
	}

	var word strings.Builder
	res := make([]string, elemNum)

	wordsCount := 0
	idx := -1
	for {
		idx++
		if str[idx] == ' ' || str[idx] == '\n' || str[idx] == '\t' {
			continue
		} else {
			for {
				if str[idx] == ' ' {
					res[wordsCount] = word.String()
					wordsCount++
					word.Reset()
					break
				}
				word.WriteByte(str[idx])
				idx++
				if idx > len(str)-1 {
					return res
				}
			}
			if wordsCount == elemNum {
				return res
			}
		}
		if idx > len(str)-1 {
			return res
		}
	}
}

// FillTheData checks entered data for correctness of types
// and returned the map with them
func FillTheData(fields []string, values []string, tableSchema map[string]interface{}) (filledData map[string]interface{}, err error) {
	filledData = make(map[string]interface{})

	for idx, field := range fields {
		_, ok := tableSchema[field]
		if !ok {
			return nil, fmt.Errorf("unknown field: <%s>", field)
		}

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
		return Comparator{}, fmt.Errorf("unknown type field in where expression: type %s can't supported", whereExpr[1])
	}

	return NewComparator(whereExpr[0], value, OperatorsMap[whereExpr[1]]), nil
}

func NewWhereExpr(condition string) []string {
	///
	return nil
}
