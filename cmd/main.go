package main

import (
	"fmt"
	"subd/internal/db"
	"time"
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

	start := time.Now()

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

	for i := 0; i < 2500000; i++ {
		database.Insert("users", map[string]interface{}{
			"name": "pasha",
			"age":  18,
			"job":  "student",
		})
	}

	elapsed := time.Since(start)
	fmt.Printf("insert took %s", elapsed)

	c1 := db.NewComparator("name", "pasha", "eq")
	c2 := db.NewComparator("age", 18, "le")

	start = time.Now()

	data := database.SelectWhere("users", []db.Comparator{c1, c2})
	for _, d := range data {
		fmt.Println("\n")
		fmt.Println(d)
	}

	elapsed = time.Since(start)
	fmt.Printf("search took %s", elapsed)

}
