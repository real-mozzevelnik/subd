package socket_server

import (
	"encoding/json"
)

type Request struct {
	Action string           `json:"action"`
	ReqId  string           `json:"reqId"`
	Data   *json.RawMessage `json:"data"`
}
