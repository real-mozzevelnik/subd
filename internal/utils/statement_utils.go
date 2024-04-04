package utils

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"subd/internal/parser/errors"
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

func TypeValidation(value string, typeValue interface{}) (convertedValue interface{}, ok error) {
	switch typeValue {
	case "INTEGER", "integer":
		convertedValue, ok = strconv.ParseInt(value, 10, 0)

	case "FLOAT":
		convertedValue, ok = strconv.ParseFloat(value, 64)

	case "BOOL", "bool":
		convertedValue, ok = strconv.ParseBool(value)

	case "TEXT", "text":
		if value[0] == '\'' && value[len(value)-1] == '\'' {
			convertedValue = strings.Replace(value, "'", "", -1)
			break
		}
		if value[0] == '"' && value[len(value)-1] == '"' {
			convertedValue = strings.Replace(value, "\"", "", -1)
			break
		}
		ok = fmt.Errorf("")

	default:
		return nil, fmt.Errorf("unknown type field for <%s>", value)
	}

	if ok != nil {
		return nil, fmt.Errorf("invalid type for <%s>. Field has %s type, but value isn't", value, typeValue.(string))
	}

	return convertedValue, nil
}

func ProcessWhereExpr(strExpr string, tableshema map[string]interface{}) (comparators []Comparator, err *errors.Error) {
	if strExpr == "" {
		return nil, &errors.Error{
			Msg:  "empty where expression",
			Code: errors.INVALID_REQUEST,
		}
	}

	strExpr = strings.TrimSuffix(strings.TrimPrefix(strExpr, "("), ")")
	strExpr = TrimMultiplieSpaces(strExpr)

	re := regexp.MustCompile(`(?s)\s+(?i)(and)(?s)\s+`)
	fieldsExpr := re.Split(strExpr, -1)

	comparators = make([]Comparator, 0)
	for _, elem := range fieldsExpr {
		cmp, err := NewCompratorByExpr(elem, tableshema)
		if err != nil {
			return nil, err
		}

		comparators = append(comparators, cmp)
	}

	return comparators, nil
}

func NewCompratorByExpr(expr string, tableschema map[string]interface{}) (Comparator, *errors.Error) {
	exprFields := SplitTrim(expr, " ", "\t", "\n")
	if len(exprFields) != 3 {
		return Comparator{}, &errors.Error{
			Msg:  fmt.Sprintf("Incorrst where expr: %s", expr),
			Code: errors.INVALID_REQUEST,
		}
	}

	value, ok := TypeValidation(exprFields[2], tableschema[exprFields[0]])
	if ok != nil {
		return Comparator{}, &errors.Error{
			Msg:  ok.Error(),
			Code: errors.INVALID_REQUEST,
		}
	}

	var msg string
	switch exprFields[1] {
	case ">=", "<=", ">", "<":
		switch tableschema[exprFields[0]] {
		case "TEXT", "text":
			ok = fmt.Errorf("")
			msg = fmt.Sprintf("this operator [%s] is not available for string comparison: %v", exprFields[1], exprFields)
		}

	case "!=", "==":
		// all right

	default:
		ok = fmt.Errorf("")
		msg = fmt.Sprintf("unknwon operator [%s] in: %v", exprFields[1], exprFields)
	}

	if ok != nil {
		return Comparator{}, &errors.Error{
			Msg:  msg,
			Code: errors.INVALID_REQUEST,
		}
	}

	return NewComparator(exprFields[0], value, OperatorsMap[exprFields[1]]), nil
}

func TrimMultiplieSpaces(str string) string {
	var buf strings.Builder

	strLen := len(str) - 1
	for idx := 0; idx < strLen; idx++ {
		if str[idx] == ' ' && str[idx+1] == ' ' {
			continue
		}
		buf.WriteByte(str[idx])
	}

	if str[strLen] != ' ' {
		buf.WriteByte(str[strLen])
	}

	return buf.String()
}
