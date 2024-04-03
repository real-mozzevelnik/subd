package errors

import "fmt"

type errorCode string

const (
	NOT_FOUND_DATA  errorCode = "not found"
	INVALID_REQUEST errorCode = "invalid request"
)

type Error struct {
	Msg  string
	Code errorCode
	Req  string
}

func (e *Error) Error() string {
	return fmt.Sprintf("\nCode: %s\nReq: %s\nMsg: %s", e.Code, e.Req, e.Msg)
}
