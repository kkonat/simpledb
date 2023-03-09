package simpledb

import (
	"testing"
)

func TestNew(t *testing.T) {
	d, err := New[Person]("testdb")
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

func TestNewCloseOpen(t *testing.T) {
	db1, err := New[Person]("testdb")
	if err != nil {
		t.Errorf("failed to create database: %v", err)
	}
	_, err = db1.Append(testData[0])
	if err != nil {
		t.Errorf("Append fail")
	}
	id2, err := db1.Append(testData[1])
	if err != nil {
		t.Errorf("Append fail")
	}
	db1.Close()

	db2, err := New[Person]("testdb")
	if err != nil {
		t.Errorf("failed to open database: %v", err)
	}
	if db1.CurrOffset != db2.CurrOffset {
		t.Error("different offsets")
	}
	if db1.ItemCounter != db2.ItemCounter {
		t.Error("differenc counters")
	}
	d, err := db2.Get(id2)
	if err != nil {
		t.Error("error getting item ", id2, err)
	}
	if d.Data.Age != testData[1].Age {
		t.Error("Wrong data")
	}
}

func TestAppend(t *testing.T) {

	db, err := New[Person]("testdb")
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
