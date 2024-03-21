package main

import (
	"fmt"
	"subd/internal/db"
	"subd/internal/parser/parser"
	"time"
)

func main() {
	database := createDB()
	parser := parser.New(database)

	// timingTest(parser)

	sql := `UPDATE users (SET job = 'popa', age = 12) WHERE name == 'andrey';
	 		UpDATE   users SeT job = 'voka', age = 16`
	parser.Accept(sql)
	parser.Execute()

	dropDB(database)
}

func timingTest(requestParser *parser.Parser) {
	insRequest := "InSeRt InTo users	(name, age, job)VaLuEs		('Sanya', 10, 'dev');"
	insCount := 200000

	start := time.Now()
	for i := 0; i < insCount; i++ {
		requestParser.Accept(insRequest)
		requestParser.Execute()
	}

	elapsed := time.Since(start)
	fmt.Printf("\n\n%d Insert time execution (sec): %f. \n Insert per second: %f", insCount, elapsed.Seconds(), float64(insCount)/elapsed.Seconds())

	// SELECT

	selRequest := "SELECT name FROM users"
	selCount := 100

	start = time.Now()
	for i := 0; i < selCount; i++ {
		requestParser.Accept(selRequest)
		requestParser.Execute()
	}

	elapsed = time.Since(start)
	fmt.Printf("\n\n%d Select time execution (sec): %f. \n Select per second: %f", selCount, elapsed.Seconds(), float64(selCount)/elapsed.Seconds())

	// SELECT WHERE

	selWhereRequest := "SELECT name FROM users WHERE name == 'clown'"
	selWhereCount := 100

	start = time.Now()
	for i := 0; i < selCount; i++ {
		requestParser.Accept(selWhereRequest)
		requestParser.Execute()
	}

	elapsed = time.Since(start)
	fmt.Printf("\n\n%d SelectWhere time execution (sec): %f. \n SelectWhere per second: %f", selWhereCount, elapsed.Seconds(), float64(selWhereCount)/elapsed.Seconds())
}

func createDB() *db.DB {
	database := db.NewDB()
	parser := parser.New(database)

	sql := `
		create table users (
			name TEXT,
			age INTEGER,
			job TEXT
		);

		insert into users(name, age, job) values('vadim', 54, 'antifriz');
			
		insert into users(name, age, job) values('andrey', 10, 'clown');
			
		insert into users(name, age, job) values('anton', 50, 'ded');
			
		insert into users(name, age, job) values('sanya', 10, 'clown');
			
		insert into users(name, age) values('nikita', 90);
	`

	parser.Accept(sql)
	parser.Execute()

	return database
}

func dropDB(database *db.DB) {
	parser := parser.New(database)
	parser.Accept(`drop table users`)
	parser.Execute()
}
