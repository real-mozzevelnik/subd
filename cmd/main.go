package main

import (
	"fmt"
	"subd/internal/db"
)

func main() {
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

	database.Insert("users", map[string]interface{}{
		"name": "vasya",
		"age":  32,
		"job":  "cook",
	})

	database.Insert("users", map[string]interface{}{
		"name": "ilya",
		"age":  14,
		"job":  "child",
	})

	database.Insert("users", map[string]interface{}{
		"name": "vasya",
		"age":  14,
		"job":  "ded",
	})

	for i := 0; i < 250000; i++ {
		database.Insert("users", map[string]interface{}{
			"name": "pasha",
			"age":  18,
			"job":  "student",
		})
	}

	c1 := db.NewComparator("name", "vasya", "eq")
	c2 := db.NewComparator("age", 18, "gt")

	data := database.SelectWhere("users", []db.Comparator{c1, c2})

	for _, d := range data {
		fmt.Println(d)
	}

	database.CreateIndex("users", "age")
	database.DeleteWhere("users", []db.Comparator{c1})

	data = database.SelectWhere("users", []db.Comparator{c2})

	for _, d := range data {
		fmt.Println(d)
	}

	database.DropIndex("users", "age")

	database.DropTable("users")

}
