package utils

import (
	"fmt"
	"log"
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

// CutSpacesFromEnds remove spaces from ends of string
func CutSpacesFromEnds(str string) string {
	if len(str) < 2 {
		return str
	}

	var begin, end int
	for idx := len(str) - 1; idx > 0; idx-- {
		if str[idx] == ' ' || str[idx] == '\t' || str[idx] == '\n' || str[idx] == ';' {
			continue
		} else {
			end = idx + 1
			break
		}
	}

	for idx := 0; idx < end; idx++ {
		if str[idx] == ' ' || str[idx] == '\t' || str[idx] == '\n' {
			continue
		} else {
			begin = idx
			break
		}
	}

	return str[begin:end]
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

// func SplitTrim(str, sep string, cutsets ...byte) []string {
// 	var buf strings.Builder
// 	var badbit bool

// 	for idx := 0; idx < len(str); idx++ {

// 		for _, cutset := range cutsets {
// 			if str[idx] == cutset {
// 				badbit = true
// 				break
// 			}
// 		}

// 		if badbit {
// 			badbit = false
// 			continue
// 		}

// 		buf.WriteByte(str[idx])
// 	}

// 	if sep != "" {
// 		return strings.Split(buf.String(), sep)
// 	} else {
// 		return append(make([]string, 1), buf.String())
// 	}
// }

// FieldsN splits the string str around each instance of one or more space characters,
// count elements in slice controls by second param elemNum
func FieldsN(str string, elemNum int) []string {
	if str == "" {
		log.Fatal("")
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
				if idx == len(str) {
					res[wordsCount] = word.String()
					wordsCount++
					return res
				}
			}
			if wordsCount == elemNum {
				return res
			}
		}
		if idx > len(str) {
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

	case "BOOL":
		convertedValue, err = strconv.ParseBool(value)

	case "FLOAT":
		convertedValue, err = strconv.ParseFloat(value, 64)

	case "TEXT":
		if value[0] == '\'' && value[len(value)-1] == '\'' {
			convertedValue = strings.Replace(value, "'", "", -1)
			break
		}
		if value[0] == '"' && value[len(value)-1] == '"' {
			convertedValue = strings.Replace(value, "\"", "", -1)
			break
		}
		err = fmt.Errorf("")

	default:
		return nil, fmt.Errorf("unknown type field for <%s>", value)
	}

	if err != nil {
		return nil,
			fmt.Errorf("invalid type for <%s>. Field has %s type, but value isn't", value, typeValue.(string))
	}

	return convertedValue, nil
}

// REFACTOR
func NewComparatorByWhereExpr(expr []string, tableSchema map[string]interface{}) (cmp Comparator, err error) {
	// for _, e := range expr {
	// 	fmt.Printf("_%s_\n", e)
	// }
	if len(expr) != 3 {
		return Comparator{}, fmt.Errorf("invalid where expression: %s", expr)
	}

	var value interface{}
	var cause, msg string

	switch tableSchema[expr[0]] {
	case "INTEGER":
		value, err = strconv.ParseInt(expr[2], 10, 0)
		if err != nil {
			cause = "type"
			msg = "Field has INTEGER type, but value isn't"
		}

	case "TEXT":
		switch expr[1] {
		case "==", "!=":
			if expr[2][0] == '\'' && expr[2][len(expr[2])-1] == '\'' {
				value = strings.Replace(expr[2], "'", "", -1)
				break
			}

			if expr[2][0] == '"' && expr[2][len(expr[2])-1] == '"' {
				value = strings.Replace(expr[2], "\"", "", -1)
				break
			}

			cause = "type"
			msg = "Field has TEXT type, but value isn't"

		default:
			cause = "operator"
			msg = "TEXT can't be compared using it"
		}

	case "FLOAT":
		value, err = strconv.ParseFloat(expr[2], 64)
		if err != nil {
			cause = "type"
			msg = "Field has FLOAT type, but value isn't"
		}

	case "BOOL":
		value, err = strconv.ParseBool(expr[2])
		if err != nil {
			cause = "type"
			msg = "Field has BOOL type, but value isn't"
		}

	default:
		cause = "type"
		msg = "Unknown type"
	}

	if err != nil {
		return Comparator{}, fmt.Errorf("invalid %s in where expression: %s\n%s", cause, expr, msg)
	}

	return NewComparator(expr[0], value, OperatorsMap[expr[1]]), nil
}

func Newexpr(condition string) []string {
	///
	return nil
}
