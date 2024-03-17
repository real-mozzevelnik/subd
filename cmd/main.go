package main

import (
	"fmt"
	"subd/internal/db"
	"subd/internal/parser/parser"
	"time"
)

func main() {
	database := createDB()
	requestParser := parser.New("", database)

	requestsTest(requestParser)

	/*
		before tests it is necessary to comment the logs block in Insert/Select Execute() method
	*/
	//timingTest(requestParser)

	dropDB(database)
}

func requestsTest(requestParser *parser.Parser) {
	request := `
		INSERT INTO users (name, age, job) VALUES (Sanya, 19, dev);
		
		INSERT INTO users (name, age, job) 
		VALUES(Vadim, 
			46, 
			dev
		);
		
		INSERT INTO users (name, age, job) VALUES      (Bob, 89, dev);

		SELECT name FROM users;

		SELECT name FROM users WHERE job == clown AND name != andrey;

		DELETE FROM users WHERE job == clown AND name == andrey;

		SELECT name FROM users
	`

	requestParser.Accept(request)

	pullLen, pullRequest := parser.StatementHandler(requestParser)

	fmt.Printf("pullLen = %d\n", pullLen)

	for i := 0; i < pullLen; i++ {
		pullRequest()
	}
}

func timingTest(requestParser *parser.Parser) {
	insRequest := "INSERT INTO users (name, age, job) VALUES (Sanya, 10, dev);"
	insCount := 200000

	start := time.Now()
	for i := 0; i < insCount; i++ {
		requestParser.Accept(insRequest)

		pullLen, pullRequest := parser.StatementHandler(requestParser)
		for i := 0; i < pullLen; i++ {
			pullRequest()
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("\n\n%d Insert time execution (sec): %f. \n Insert per second: %f", insCount, elapsed.Seconds(), float64(insCount)/elapsed.Seconds())

	// SELECT

	selRequest := "SELECT name FROM users"
	selCount := 100

	start = time.Now()
	for i := 0; i < selCount; i++ {
		requestParser.Accept(selRequest)

		pullLen, pullRequest := parser.StatementHandler(requestParser)
		for i := 0; i < pullLen; i++ {
			pullRequest()
		}
	}

	elapsed = time.Since(start)
	fmt.Printf("\n\n%d Select time execution (sec): %f. \n Select per second: %f", selCount, elapsed.Seconds(), float64(selCount)/elapsed.Seconds())

	// SELECT WHERE

	selWhereRequest := "SELECT name FROM users WHERE job == clown AND name == vadim"
	selWhereCount := 100

	start = time.Now()
	for i := 0; i < selCount; i++ {
		requestParser.Accept(selWhereRequest)

		pullLen, pullRequest := parser.StatementHandler(requestParser)
		for i := 0; i < pullLen; i++ {
			pullRequest()
		}
	}

	elapsed = time.Since(start)
	fmt.Printf("\n\n%d SelectWhere time execution (sec): %f. \n SelectWhere per second: %f", selWhereCount, elapsed.Seconds(), float64(selWhereCount)/elapsed.Seconds())
}

func createDB() *db.DB {
	database := db.NewDB()

	database.Createtable(
		"users",
		map[string]interface{}{
			"name": "TEXT",
			"age":  "INTEGER",
			"job":  "TEXT",
		},
	)

	database.Insert("users", map[string]interface{}{
		"name": "vasya",
		"age":  30,
		"job":  "clown",
	})

	database.Insert("users", map[string]interface{}{
		"name": "andrey",
		"age":  10,
		"job":  "clown",
	})

	database.Insert("users", map[string]interface{}{
		"name": "anton",
		"age":  50,
		"job":  "ded",
	})

	return database
}

func dropDB(database *db.DB) {
	database.DropTable("users")
}
