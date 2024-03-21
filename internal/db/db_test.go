package db

import (
	"math/rand/v2"
	"strconv"
	"subd/internal/utils"
	"testing"
)

func TestUpdate(t *testing.T) {
	db := NewDB()
	db.CreateTable("users", map[string]interface{}{
		"name": "TEXT",
		"age":  "INTEGER",
		"job":  "TEXT",
	})
	defer db.DropTable("users")

	for i := 0; i < 100000; i++ {
		j := rand.IntN(100)
		db.Insert("users", map[string]interface{}{
			"name": "user" + strconv.Itoa(j),
			"age":  int64(j),
			"job":  "clown" + strconv.Itoa(j),
		})
	}
	for i := 0; i < 100; i++ {
		j := 1000 + i
		db.Insert("users", map[string]interface{}{
			"name": "user" + strconv.Itoa(j),
			"age":  int64(j),
			"job":  "clown" + strconv.Itoa(j),
		})
	}

	c1 := utils.NewComparator("age", int64(1000), "eq")
	data := db.SelectWhere("users", []utils.Comparator{c1}, []string{"age"})
	for _, d := range data {
		t.Log(d)
	}

	db.UpdateWhere("users", map[string]interface{}{"age": int64(2000)}, []utils.Comparator{c1})

	c1 = utils.NewComparator("age", int64(2000), "eq")
	data = db.SelectWhere("users", []utils.Comparator{c1}, []string{"age"})
	for _, d := range data {
		t.Log(d)
	}
}
