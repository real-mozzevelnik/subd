package socket_server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"runtime/debug"
)

const (
	//	4 MB
	MaxScanTokenSize = 4 * 1024 * 1024
)

type Server struct {
	Conn     net.Conn
	Reader   *bufio.Reader
	Encoder  *json.Encoder
	LineData []byte
}

func (server *Server) Read() {
	for {
		var err error
		lineData, isPrefix, err := server.Reader.ReadLine()
		if err == io.EOF {
			log.Println("socket_server: server disconnected: " + server.Conn.RemoteAddr().String())
			server.Conn.Close()
			break
		}

		if err != nil {
			log.Println("socket_server: reader error: ", err)
			break
		}
		fmt.Println(2)
		if isPrefix {
			server.LineData = append(server.LineData, lineData...)

			if len(server.LineData) > MaxScanTokenSize {
				log.Println("socket_server: connection flood. closing connection.")
				server.Conn.Close()
				break
			}
			continue
		}
		server.LineData = append(server.LineData, lineData...)
		server.Decode()

		server.LineData = []byte{}
	}
}

func (server *Server) Decode() {
	var err error
	var req Request
	err = json.Unmarshal(server.LineData, &req)
	if err != nil {
		log.Println(err)

		res := Response{
			ReqId:   "error",
			Success: false,
			Error:   "JSON decode fail: " + err.Error(),
		}

		err = server.Encoder.Encode(res)
		if err != nil {
			log.Println(err)
		}
		return
	}

	defer func() {
		if r := recover(); r != nil {
			log.Println(debug.Stack())
		}
	}()

	go server.HandleRequest(&req)
}

func (server *Server) HandleRequest(req *Request) {
	var err error
	var res Response

	if req.Data == nil {
		res = Response{
			ReqId:   req.ReqId,
			Success: false,
			Error:   "no request data provided",
		}
	} else {

		data, err := Pipe.actions[req.Action].h(req.Data)
		if err != nil {
			res = Response{
				ReqId:   req.ReqId,
				Success: false,
				Error:   err.Error(),
			}
		} else {

			res = Response{
				ReqId:   req.ReqId,
				Success: true,
				Data:    data,
			}
		}
	}

	err = server.Encoder.Encode(&res)
	if err != nil {
		log.Println("socket_server: Encoder error: ", err)
	}
}
