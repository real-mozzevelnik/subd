package parser

import (
	"strings"

	"subd/internal/db"
	"subd/internal/parser/errors"
	"subd/internal/parser/statement"
	"subd/internal/utils"
)

type Parser struct {
	dataBase *db.DB
	request  string
	queue    []statement.Statement
}

func New(dataBase *db.DB) *Parser {
	return &Parser{
		dataBase: dataBase,
		queue:    make([]statement.Statement, 0),
	}
}

func (p *Parser) Accept(request string) (err *errors.Error) {
	p.request = utils.CutSpacesFromEnds(request)
	p.clearRequestAndQueue()
	return p.splitRequestAndCreateQueue()
}

func (p *Parser) Execute() (resultSet []map[string]interface{}, err *errors.Error) {
	for _, statement := range p.queue {
		if err = statement.Prepare(); err != nil {
			return nil, err
		}

		if resultSet, err = statement.Execute(); err != nil {
			return nil, err
		}
	}
	return resultSet, nil
}

func (p *Parser) splitRequestAndCreateQueue() (err *errors.Error) {
	splitRequests := utils.SplitTrim(p.request, ";")

	for _, request := range splitRequests {
		request = utils.CutSpacesFromEnds(request)
		statement, err := statement.New(request, p.dataBase)
		if err != nil {
			return err
		}
		p.queue = append(p.queue, *statement)
	}

	return nil
}

func (p *Parser) clearRequestAndQueue() {
	p.queue = p.queue[:0]
	p.request = strings.TrimSuffix(p.request, ";")
}
