package main

import (
	"fmt"
	"strconv"
	"subd/internal/btree"
	"subd/internal/db"
	"time"
)

func main() {
	testDb()
}

func testBtree() {
	tree := btree.New(btree.DefaultBTreeDegree)

	for i := 0; i < 10000; i++ {
		tree.Put(i, strconv.Itoa(i))
	}

	for i := 0; i < 30; i++ {
		tree.Put(5654, "5654")
	}

	d := tree.GetByKey(5654)

	fmt.Println(d)

}

func testDb() {
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

	start := time.Now()

	for i := 0; i < 2500000; i++ {
		database.Insert("users", map[string]interface{}{
			"name": "pasha",
			"age":  18,
			"job":  "student",
		})
	}
	elapsed := time.Since(start)
	fmt.Println("insert took ", elapsed)

	database.CreateIndex("users", "name")
	database.DropIndex("users", "name")

	c1 := db.NewComparator("name", "vasya", "eq")
	c2 := db.NewComparator("age", 18, "gt")

	start = time.Now()

	data := database.SelectWhere("users", []db.Comparator{c1, c2})

	elapsed = time.Since(start)
	fmt.Println("search took ", elapsed)

	for _, d := range data {
		fmt.Println(d)
	}

	start = time.Now()

	database.DeleteWhere("users", []db.Comparator{c1})

	elapsed = time.Since(start)
	fmt.Println("delete took ", elapsed)

	start = time.Now()

	data = database.SelectWhere("users", []db.Comparator{c2})

	elapsed = time.Since(start)
	fmt.Println("search took ", elapsed)

	for _, d := range data {
		fmt.Println(d)
	}

	start = time.Now()

	database.DropTable("users")

	elapsed = time.Since(start)
	fmt.Println("drop took ", elapsed)
}
