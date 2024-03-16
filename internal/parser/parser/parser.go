package parser

import (
	"fmt"
	"strings"

	"subd/internal/parser/statement"
)

type Parser struct {
	originRequest string
	stmtQueue     []statement.Statement
}

func New(originRequest string) *Parser {
	return &Parser{
		originRequest: originRequest,
		stmtQueue:     make([]statement.Statement, 0),
	}
}

func (p *Parser) Accept(query string) error {
	query = strings.NewReplacer("\t", "", "\n", "", "  ", "", "   ", "", "    ", "").Replace(query)

	p.originRequest = query

	return nil
}

func (p *Parser) Prepare() error {

	subRequests := strings.Split(p.originRequest, ";")

	for i := 0; i < len(subRequests); i++ {
		stmt := statement.New(subRequests[i])
		p.stmtQueue = append(p.stmtQueue, *stmt)
	}

	for i := 0; i < len(p.stmtQueue)-1; i++ {
		fmt.Printf("%d : %s\n", i, p.stmtQueue[i].OriginText())
	}

	return nil
}

func (p *Parser) Execute() (map[string]string, error) {
	/*
		(1) Call SQL statements handlers
		(2) Generate a result set OR
		    manipulate with data
	*/
	return nil, nil
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
