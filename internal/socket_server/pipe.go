package socket_server

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
)

type SocketPipe struct {
	actions map[string]Action
	port    string
}

type Action struct {
	h       Handler
	pattern string
}

type Handler func(*json.RawMessage) (map[string]interface{}, error)

func newSocketPipe() *SocketPipe {
	return &SocketPipe{
		actions: make(map[string]Action),
	}
}

var Pipe = newSocketPipe()

func Handle(action string, handler Handler) {
	if action == "" {
		panic("socket_server: action can't be an empty string")
	}
	if handler == nil {
		panic("socket_server: nil handler")
	}
	if _, ok := Pipe.actions[action]; ok {
		panic("socket_server: action " + action + " already exists")
	}

	Pipe.actions[action] = Action{pattern: action, h: handler}
}

func ListenAndServe(port string) {
	ln, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()

	Pipe.port = port
	log.Println("socket_server: listening on localhost" + port)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}

		server := Server{
			Conn:    conn,
			Reader:  bufio.NewReader(conn),
			Encoder: json.NewEncoder(conn),
		}

		log.Println("socket_server: new connection from " + server.Conn.RemoteAddr().String())

		go server.Read()
	}
}
