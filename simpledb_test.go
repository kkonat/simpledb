package simpledb

import (
	"testing"

	log "github.com/sirupsen/logrus"
)

func TestNew(t *testing.T) {
	d, err := Connect[Person]("testdb")
	if err != nil {
		t.Errorf("failed to create database: %v", err)
	}
	d.Close()
}

type Person struct {
	Name    string
	Surname string
	Age     uint
}

var testData []Person = []Person{
	{"Hans", "Kloss", 44},
	{"Ulrika", "Van der Klompfer", 666},
}

func TestKill(t *testing.T) {
	db1, err := Connect[Person]("testdb")
	if err != nil {
		t.Errorf("failed to create database: %v", err)
	}
	err = db1.Kill("testdb")
	if err != nil {
		t.Errorf("failed to kill database: %v", err)
	}
}
func TestNewCloseOpen(t *testing.T) {
	db1, _ := Connect[Person]("testdb")
	db1.Kill("testdb")

	var err error
	db1, err = Connect[Person]("testdb")
	if err != nil {
		t.Errorf("failed to create database: %v", err)
	}

	id1, err := db1.Append(testData[0])
	if err != nil {
		t.Errorf("Append fail")
	}
	if id1 != 0 {
		t.Error("Bad id")
	}
	id2, err := db1.Append(testData[1])
	if err != nil {
		t.Errorf("Append fail")
	}
	if id2 != 1 {
		t.Error("Bad id")
	}
	db1.Close()

	db2, err := Connect[Person]("testdb")
	if err != nil {
		t.Errorf("failed to open database: %v", err)
	}
	if db1.CurrOffset != db2.CurrOffset {
		t.Error("different offsets")
	}
	if db1.ItemCounter != db2.ItemCounter {
		t.Error("differenc counters")
	}
	pers, err := db2.Get(id2)
	if err != nil {
		t.Error("error getting item ", id2, err)
	}
	if pers.Age != testData[id2].Age {
		t.Error("Wrong data")
	}
	_, err = db2.Get(9999)
	if err == nil {
		t.Error("got non existing object")
	}
	hr, err := db2.MemCache.hitRate()
	if hr != 0 {
		log.Infof("Cache hit rate %.2f, %v", hr*100, err)
		t.Error("wrong hit rate")
	}
	_, _ = db2.Get(id2)
	hr, _ = db2.MemCache.hitRate()
	if hr != 0.5 {
		log.Infof("Cache hit rate %f, %v", hr, err)
		t.Error("wrong hit rate")
	}
	_, _ = db1.Get(id2)
	hr, _ = db1.MemCache.hitRate()
	if hr != 1 {
		log.Infof("Cache hit rate %f, %v", hr, err)
		t.Error("wrong hit rate")
	}
}

func TestAppend(t *testing.T) {

	db, err := Connect[Person]("testdb")
	if err != nil {
		t.Errorf("failed to create database: %v", err)
	}
	defer db.Close()

	for _, p := range testData {
		_, err := db.Append(p)
		if err != nil {
			t.Error("failed to add item")
		}
		// r, err := db.Get(id)
		// result := r.(Person)

		// if err != nil {
		// 	t.Error("failed to get  item")
		// }
		// if result.Name != p.Name ||
		// 	result.Surname != p.Surname ||
		// 	result.Age != p.Age {
		// 	t.Error("data differs")
		// }
	}
	db.Flush()
}
