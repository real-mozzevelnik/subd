package parser

import (
	"fmt"
	"subd/internal/db"
	"testing"
	"time"
)

func TestSelectStatement(t *testing.T) {
	db := createDB()
	parser := New(db)

	sql := `select name, age, salary, alive from users`
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

	selectData(db, t)

	sql := `select 
	name
	
	, 
	 age
	  ,
	     job
		   ,  
		       salary   , alive
	 FROM users WHERE salary 
	 
	    >
		   10
		   ;    `
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

	sql := `update users 
      set 
	   name 
	     =   
		   'test'   , 
		     age   =    0
			   ;
			 `
	parser.Accept(sql)

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

	sql := `UPDATE users SET name = 'test'
	  WHERE
	     age > 10   
		 `
	t.Logf("request: %s\n", sql)

	parser.Accept(sql)
	_, err := parser.Execute()
	if err != nil {
		panic(err)
	}

	selectData(db, t)
}

func TestDeleteStatement(t *testing.T) {
	db := createDB()
	parser := New(db)

	selectData(db, t)

	sql := `delete from users`
	t.Logf("request: %s\n", sql)

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
	t.Logf("request: %s\n", sql)

	parser.Accept(sql)
	if _, err := parser.Execute(); err != nil {
		panic(err)
	}

	selectData(db, t)
}

func TestCreateDropTable(t *testing.T) {
	db := db.NewDB()
	parser := New(db)

	t.Log(db.Info())
	sql := `create table pozers (name TEXT, id float)`
	t.Logf("request: %s\n", sql)

	parser.Accept(sql)
	if _, err := parser.Execute(); err != nil {
		t.Fatal(err)
	}

	t.Log(db.Info())

	sql = `drop table pozers`
	t.Logf("request: %s\n", sql)

	parser.Accept(sql)
	if _, err := parser.Execute(); err != nil {
		t.Fatal(err)
	}

	t.Log(db.Info())

}

func TestInsertStatement(t *testing.T) {
	db := createDB()
	parser := New(db)

	selectData(db, t)

	sql := `INSERT INTO users(age, name, salary, alive) VALUES (15, "vadik", 12.2, f);
	
	    INSERT INTO		 
	              users(
		 age 
		  , name  	, 
		  
		 
		  salary  ,
		     alive    )
			 
			 VALUES   (   
				 21, 
				 
				   "bobik", 
				   18.5, f 
				     	);
	INSERT INTO users(age, name) VALUES (90, "lena");`

	t.Logf("request: %s\n", sql)

	parser.Accept(sql)
	if _, err := parser.Execute(); err != nil {
		panic(err)
	}

	selectData(db, t)
}

func TestIndex(t *testing.T) {
	db := db.NewDB()
	parser := New(db)

	parser.Accept(`create table test_table (id integer, name text)`)
	parser.Execute()

	fmt.Println("data base info: ", db.Info())

	sql := `create index test_table on name`
	t.Logf("request: %s", sql)

	parser.Accept(sql)
	if _, err := parser.Execute(); err != nil {
		panic(err)
	}

	fmt.Println("data base info: ", db.Info())

	sql = `drop index test_table on name`
	t.Logf("request: %s\n", sql)

	parser.Accept(sql)
	if _, err := parser.Execute(); err != nil {
		panic(err)
	}

	fmt.Println("data base info: ", db.Info())
}

func TestTiming(t *testing.T) {
	database := createDB()
	requestParser := New(database)

	insRequest := `insert into users (name, age, job) values ('anton', 50, 'ded')`
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

	selWhereRequest := "SELECT name FROM users WHERE age > 15"
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

	sql := `create table users (name TEXT, age INTEGER, job TEXT, salary FLOAT, alive BOOL)`

	parser.Accept(sql)
	_, err := parser.Execute()
	if err != nil {
		fmt.Println(err)
		panic("")
	}

	sql = `INSERT INTO users(age, name, salary, alive) VALUES (15, "vadik", 12.2, f);
	INSERT INTO users(age, name, salary, alive) VALUES (21, "bobik", 18.5, f);
	INSERT INTO users(age, name) VALUES (90, "lena")`

	// sql = `insert into users (name, age, job, salary, alive)   values ('andrey', 10, 'clown', '12', false);

	// insert into users (name) values ('anton');

	// insert into users (name, age, job) values ('sanya', 10, 'clown');

	// insert into users (name, age, job) values ('nikita', 90, 'ded')`

	parser.Accept(sql)
	parser.Execute()

	return database
}

func selectData(db *db.DB, t *testing.T) {
	parser := New(db)

	parser.Accept("SELECT name, age, job, salary, alive FROM users")
	data, err := parser.Execute()

	if err != nil {
		t.Log(err)
	} else {
		for idx, data := range data {
			t.Logf("%d: %v\n", idx, data)
		}
	}
	t.Log("-----------------")
}
