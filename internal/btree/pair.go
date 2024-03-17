package btree

import "fmt"

type KeyType any

type ValueType []string

type pair struct {
	Key   KeyType
	Value ValueType
}

func keyLessThan(key, other KeyType) bool {
	return fmt.Sprint(key) < fmt.Sprint(other)
}

func keyEqualTo(key, other KeyType) bool {
	return key == other
}

func newPair(key KeyType, value string) pair {
	data := make(ValueType, 0)
	data = append(data, value)
	return pair{
		Key:   key,
		Value: data,
	}
}
