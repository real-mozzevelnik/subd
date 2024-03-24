package parser

import (
	"subd/internal/db"
	"testing"
	"time"
)

func TestSelectStatement(t *testing.T) {
	db := createDB()
	parser := New(db)

	sql := `select (name, age) from users`
	t.Logf("request: %s\n", sql)

	parser.Accept(sql)
	data, err := parser.Execute()
	if err != nil {
		t.Log("err:", err)
	} else {
		for _, d := range data {
			t.Log(d)
		}
	}
}

func TestSelectWhereStatement(t *testing.T) {
	db := createDB()
	parser := New(db)

	sql := `select (name, age) from users where age > 10`
	t.Logf("request: %s\n", sql)

	parser.Accept(sql)
	data, err := parser.Execute()
	if err != nil {
		t.Log("err:", err)
	} else {
		for _, d := range data {
			t.Log(d)
		}
	}
}

func TestUpdateStatement(t *testing.T) {
	db := createDB()
	parser := New(db)

	selectData(db, t)

	sql := `update users set name = 'test', age = 0`
	t.Logf("request: %s\n", sql)

	parser.Accept(sql)
	_, err := parser.Execute()
	if err != nil {
		panic(err)
	} else {
		selectData(db, t)
	}
}

func TestUpdateWhereStatement(t *testing.T) {
	db := createDB()
	parser := New(db)

	selectData(db, t)

	parser.Accept("UPDATE users SET name = 'test' WHERE age == 10")
	_, err := parser.Execute()
	if err != nil {
		panic(err)
	}

	selectData(db, t)
}

func TestInsertStatement(t *testing.T) {
	db := createDB()
	parser := New(db)

	selectData(db, t)

	sql := `INSERT INTO users(age, name) VALUES (15, "putin")`
	parser.Accept(sql)
	if _, err := parser.Execute(); err != nil {
		panic(err)
	}

	selectData(db, t)
}

func TestDeleteStatement(t *testing.T) {
	db := createDB()
	parser := New(db)

	selectData(db, t)

	sql := `delete from users`
	parser.Accept(sql)
	if _, err := parser.Execute(); err != nil {
		panic(err)
	}

	selectData(db, t)
}

func TestDeleteWhereStatement(t *testing.T) {
	db := createDB()
	parser := New(db)

	selectData(db, t)

	sql := `delete from users where age > 25`
	parser.Accept(sql)
	if _, err := parser.Execute(); err != nil {
		panic(err)
	}

	selectData(db, t)
}

func TestTiming(t *testing.T) {
	database := createDB()
	requestParser := New(database)

	insRequest := "InSeRt InTo users	(name, age, job) VaLuEs		('Sanya', 10, 'dev');"
	insCount := 200000

	start := time.Now()
	for i := 0; i < insCount; i++ {
		requestParser.Accept(insRequest)
		requestParser.Execute()
	}

	elapsed := time.Since(start)
	t.Logf("\n\n%d Insert time execution (sec): %f. \n Insert per second: %f", insCount, elapsed.Seconds(), float64(insCount)/elapsed.Seconds())

	selRequest := "SELECT name FROM users"
	selCount := 100

	start = time.Now()
	for i := 0; i < selCount; i++ {
		requestParser.Accept(selRequest)
		requestParser.Execute()
	}

	elapsed = time.Since(start)
	t.Logf("\n\n%d Select time execution (sec): %f. \n Select per second: %f", selCount, elapsed.Seconds(), float64(selCount)/elapsed.Seconds())

	selWhereRequest := "SELECT name FROM users WHERE name == 'clown'"
	selWhereCount := 100

	start = time.Now()
	for i := 0; i < selCount; i++ {
		requestParser.Accept(selWhereRequest)
		requestParser.Execute()
	}

	elapsed = time.Since(start)
	t.Logf("\n\n%d SelectWhere time execution (sec): %f. \n SelectWhere per second: %f", selWhereCount, elapsed.Seconds(), float64(selWhereCount)/elapsed.Seconds())
}

func createDB() *db.DB {
	database := db.NewDB()
	parser := New(database)

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

		insert into users(name, age, job) values('nikita', 90, 'ded')
	`

	parser.Accept(sql)
	parser.Execute()

	return database
}

func selectData(db *db.DB, t *testing.T) {
	parser := New(db)

	parser.Accept("SELECT name, age, job FROM users")
	data, err := parser.Execute()

	if err != nil {
		t.Log(err)
	} else {
		for _, data := range data {
			t.Log(data)
		}
	}
	t.Log("____________")
}
