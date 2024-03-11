package btree

type KeyType string

type ValueType any

type pair struct {
	Key   KeyType
	Value ValueType
}

func (key KeyType) lessThan(other KeyType) bool {
	return key < other
}

func (key KeyType) equalTo(other KeyType) bool {
	return key == other
}

func newPair(key KeyType, value ValueType) pair {
	return pair{
		Key:   key,
		Value: value,
	}
}
