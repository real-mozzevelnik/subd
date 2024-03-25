package parser

import (
	"strings"

	"subd/internal/db"
	"subd/internal/parser/errors"
	"subd/internal/parser/statement"
	"subd/internal/utils"
)

type Parser struct {
	dataBase       *db.DB
	originRequest  string
	statementQueue []statement.Statement
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

func (p *Parser) Execute() (resultSet []map[string]interface{}, err *errors.Error) {
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
	splitRequests := utils.SplitTrim(p.originRequest, ";", "\t", "\n")

	for _, request := range splitRequests {
		statement, err := statement.New(request, p.dataBase)
		if err != nil {
			panic(err)
		}
		p.statementQueue = append(p.statementQueue, *statement)
	}

	return nil
}

func (p *Parser) clearRequestAndQueue() {
	p.statementQueue = p.statementQueue[:0]
	p.originRequest = strings.TrimSuffix(p.originRequest, ";")
}
