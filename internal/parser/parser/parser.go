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
}

// forming a queue of statements
func (p *Parser) Prepare() error {
	//REFACTOR: Is it right way to clear a ' sign from oroginal request and check of type
	//			in INSERT Execute() method?
	query := strings.NewReplacer("\t", "", "\n", "", ", ", ",").Replace(p.originRequest)

	//split request by ';'
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

/*
TODO: сделать функцию, которая за 1 вызов доастет из очереди запрос,
выполняет его, возвращает db.Result или nil по окончании запросов.
Если SQL запрос не подразумевает получение множество с выборкой
данных, то возвращается пустое множество.
*/

func (p *Parser) Execute() (map[string]string, error) {
	for _, statement := range p.statementQueue {
		(*statement).Execute()
	}

	return nil, nil
}

func StatementHandler(parser *Parser) func() *db.Result {
	return func() *db.Result {
		return nil
	}
}

func (p *Parser) OriginText() string {
	return p.originRequest
}

// Truncate Truncate
// Drop     Drop
// Create   Create
// Rename   Rename
// Alter    Alter

// Select Select
// Insert Insert
// Delete Delete
// Merge  Merge
// Update Update

// Grant  Grant
// Revoke Revoke
// Deny   Deny

// BeginTC    BeginTC
// CommitTC   CommitTC
// RollbackTC RollbackTC
// SaveTc     SaveTC
