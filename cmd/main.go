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

	sql := `
			insert into users(name, age, job) values('vadim', 54, 'antifriz');
			
			insert into users(name, age, job) values('andrey', 10, 'clown');
			
			insert into users(name, age, job) values('anton', 50, 'ded');
			
			insert into users(name, age, job) values('sanya', 10, 'clown');
			
			insert into users(name, age, job) values('nikita', 90, 'ded');
	`

	// timingTest(parser)

	parser.Accept(sql)
	parser.Execute()

	// sqlSelect := `SELECT (name, age, job) FROM users`
	sqlSelect := `CREATE INDEX users ON name`
	parser.Accept(sqlSelect)
	data := parser.Execute()
	fmt.Println("\nResult set:")
	for _, d := range data {
		fmt.Printf("%v\n", d)
	}

	// sql = `DELETE name FROM users;`
	// parser.Accept(sql)
	// parser.Execute()

	parser.Accept(sqlSelect)
	data = parser.Execute()
	fmt.Println("\nResult set:")
	for _, d := range data {
		fmt.Printf("%v\n", d)
	}

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
	`
	parser.Accept(sql)
	parser.Execute()

	return database
}

func dropDB(database *db.DB) {
	parser := parser.New(database)

	sql := `drop index users on name; drop table users;`
	parser.Accept(sql)
	parser.Execute()
}
