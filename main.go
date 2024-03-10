package main

import (
	"fmt"
	"subd/internal/btree"
	"time"
)

func main() {
	start := time.Now()

	b := btree.NewTree(btree.DefaultMinItems)
	b.Put("vasya", map[string]interface{}{"age": 30, "class": "child"})
	b.Put("anton", map[string]interface{}{"age": 29, "class": "parent"})
	b.Put("gandon", map[string]interface{}{"age": 10, "class": "adult"})
	b.Put("vasya", map[string]interface{}{"age": 31, "class": "child"})
	for j := 0; j < btree.DefaultMinItems*200; j++ {
		b.Put("vasya", map[string]interface{}{"age": 30, "class": "child"})
		// fmt.Println(j)
	}

	elapsed := time.Since(start)
	fmt.Printf("insert 2 560 000 took %s", elapsed)

	start = time.Now()

	c := btree.Comparator{
		FieldName: "age",
		Operation: "lt",
		Value:     31,
	}
	c1 := btree.Comparator{
		FieldName: "class",
		Operation: "eq",
		Value:     "adult",
	}
	items := b.FindByValue([]btree.Comparator{c, c1})
	for _, i := range items {
		fmt.Println("\n")
		fmt.Println(*i)
	}

	elapsed = time.Since(start)
	fmt.Printf("search took %s", elapsed)
}
