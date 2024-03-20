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

func New(dataBase *db.DB) *Parser {
	return &Parser{
		DataBase:       dataBase,
		statementQueue: make([]*statement.Statement, 0),
	}
}

func (p *Parser) Accept(request string) {
	p.originRequest = request
	p.prepare()
}

func (p *Parser) Execute() []map[string]interface{} {
	index := 0
	for index = 0; index < len(p.statementQueue)-1; index++ {
		(*p.statementQueue[index]).Prepare()
		(*p.statementQueue[index]).Execute()
	}
	//return map[string]interface{} last statement
	(*p.statementQueue[index]).Prepare()
	return (*p.statementQueue[index]).Execute()
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
		p.statementQueue = append(p.statementQueue, statement)
	}

	return nil
}
