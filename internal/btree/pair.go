package btree

import "fmt"

type KeyType any

type ValueType any

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

func newPair(key KeyType, value ValueType) pair {
	return pair{
		Key:   key,
		Value: value,
	}
}
