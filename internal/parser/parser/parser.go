package parser

import (
	"strings"

	"subd/internal/db"
	"subd/internal/parser/statement"
)

type Parser struct {
	DataBase       *db.DB
	originRequest  string
	statementQueue []*statement.Statement
}

func New(request string, dataBase *db.DB) *Parser {
	return &Parser{
		DataBase:       dataBase,
		originRequest:  request,
		statementQueue: make([]*statement.Statement, 0),
	}
}

func (p *Parser) Accept(request string) {
	p.originRequest = request
	p.prepare()
}

// forming a queue of statements
func (p *Parser) prepare() error {
	//clear queue
	p.statementQueue = p.statementQueue[:0]
	//clear request from trash signs
	query := strings.NewReplacer("\t", "", "\n", "", ", ", ",").Replace(p.originRequest)

	//adding ';' sign at the end of the request
	r := []rune(query)
	if r[len(r)-1] != ';' {
		query = query + string(";")
	}

	//split request by ';' sign
	subRequests := strings.Split(query, ";")

	//parse compound request on separate
	for i := 0; i < len(subRequests)-1; i++ {
		statement := statement.New(subRequests[i], p.DataBase)
		//parse statement and prepare it for execute method
		(*statement).Prepare()
		p.statementQueue = append(p.statementQueue, statement)
	}

	return nil
}

// return pullRequest function, whick pull a one request from the queue and executes it
func StatementHandler(parser *Parser) (pullLength int, pullRequest func() []*db.Row) {
	index := -1
	queueLength := len(parser.statementQueue)

	return queueLength, func() []*db.Row {
		if index < queueLength {
			index++
			return (*parser.statementQueue[index]).Execute()
		} else {
			panic("Pull request is empty")
		}
	}
}
