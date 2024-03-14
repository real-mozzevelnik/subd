package db

type Result struct {
	Key   interface{}
	Value interface{}
}

func newResult(Key interface{}, Value interface{}) *Result {
	return &Result{
		Key:   Key,
		Value: Value,
	}
}
