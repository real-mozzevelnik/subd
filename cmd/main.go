package main

import (
	"subd/internal/db"
	"subd/internal/parser/parser"
)

func main() {
	database := createDB()
	parser := parser.New("")

	request := `INSERT INTO users (name, age, job) 
	VALUES
	(
		'Vadim', 
		46, 
		'pidr'
	),
	(
		'Sanya',
		19,
		'pidr'
	);

	SELECT name FROM users WHERE job == pidr;`

	parser.Accept(request)
	parser.Prepare()
	parser.Execute()

	// data := database.SelectWhere("users", []db.Comparator{c1, c2})

	dropDB(database)
}

func createDB() *db.DB {
	database := db.NewDB()

	database.CreateTable(
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
		"job":  "child",
	})

	database.Insert("users", map[string]interface{}{
		"name": "anton",
		"age":  50,
		"job":  "ded",
	})

	return database
}

func dropDB(database *db.DB) {
	//database.DropIndex("users", "age")

	database.DropTable("users")
}

// for i := 0; i < 250000; i++ {
// 	database.Insert("users", map[string]interface{}{
// 		"name": "pasha",
// 		"age":  18,
// 		"job":  "student",
// 	})
// }

// c1 := db.NewComparator("name", "vasya", "eq")
// c2 := db.NewComparator("age", 18, "gt")

// for _, d := range data {
// 	fmt.Println(d)
// }

// database.CreateIndex("users", "age")
// database.DeleteWhere("users", []db.Comparator{c1})

// data = database.SelectWhere("users", []db.Comparator{c2})

// for _, d := range data {
// 	fmt.Println(d)
// }
