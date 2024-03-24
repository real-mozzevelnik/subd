package parser

import (
	"strings"

	"subd/internal/db"
	"subd/internal/parser/statement"
)

type Parser struct {
	dataBase       *db.DB
	originRequest  string
	statementQueue []statement.Statement
	err            chan error
}

func New(dataBase *db.DB) *Parser {
	return &Parser{
		dataBase:       dataBase,
		statementQueue: make([]statement.Statement, 0),
	}
}

func (p *Parser) Accept(request string) {
	p.originRequest = request
	p.clearRequestAndQueue()
	p.splitRequestAndCreateQueue()
}

func (p *Parser) Execute() (resultSet []map[string]interface{}, err error) {
	for _, statement := range p.statementQueue {
		if err = statement.Prepare(); err != nil {
			return nil, err
		}

		if resultSet, err = statement.Execute(); err != nil {
			return nil, err
		}
	}
	return resultSet, nil
}

func (p *Parser) splitRequestAndCreateQueue() (err error) {
	splitRequests := strings.Split(p.originRequest, ";")

	for _, request := range splitRequests {
		statement, err := statement.New(request, p.dataBase)
		if err != nil {
			return err
		}
		p.statementQueue = append(p.statementQueue, *statement)
	}

	return nil
}

func (p *Parser) clearRequestAndQueue() {
	p.statementQueue = p.statementQueue[:0]
	p.originRequest = strings.NewReplacer("\t", "", "\n", "", ", ", ",").Replace(p.originRequest)
	p.originRequest = strings.TrimSuffix(p.originRequest, ";")
}
