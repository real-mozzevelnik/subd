package db

import (
	"fmt"
	"subd/internal/btree"
)

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

func (c Comparator) toBTreeComparator() btree.Comparator {
	return btree.Comparator{
		FieldName: c.FieldName,
		Operation: c.Operation,
		Value:     c.Value,
	}
}

func NewComparator(fieldName string, value interface{}, operation string) Comparator {
	availableOperations := []string{"eq", "neq", "le", "lt", "ge", "gt"}
	inOperations := false
	for _, op := range availableOperations {
		if operation == op {
			inOperations = true
		}
	}
	if !inOperations {
		fmt.Println("available operations: eq, neq, le, lt, ge, gt")
		return Comparator{}
	}

	return Comparator{
		FieldName: fieldName,
		Value:     value,
		Operation: operation,
	}
}
