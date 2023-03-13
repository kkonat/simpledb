package simpledb

import (
	"fmt"
	"math/rand"
	"testing"

	log "github.com/sirupsen/logrus"
)

func TestNew(t *testing.T) {
	d, err := Open[Person]("testdb")
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
	db1, err := Open[Person]("testdb")
	if err != nil {
		t.Errorf("failed to create database: %v", err)
	}
	err = Destroy(db1, "testdb")
	if err != nil {
		t.Errorf("failed to kill database: %v", err)
	}
}
func TestBasicFunctionality(t *testing.T) {
	db1, _ := Open[Person]("testdb")
	Destroy(db1, "testdb")

	var err error
	db1, err = Open[Person]("testdb")
	if err != nil {
		t.Errorf("failed to create database: %v", err)
	}

	id1, err := db1.Append([]byte("Person1"), &testData[0])
	if err != nil {
		t.Errorf("Append fail")
	}
	if id1 != 0 {
		t.Error("Bad id")
	}
	id2, err := db1.Append([]byte("Person2"), &testData[1])
	if err != nil {
		t.Errorf("Append fail")
	}
	if id2 != 1 {
		t.Error("Bad id")
	}
	db1.Close()

	db2, err := Open[Person]("testdb")
	if err != nil {
		t.Errorf("failed to open database: %v", err)
	}
	if db1.currentOffset != db2.currentOffset {
		t.Error("different offsets")
	}
	if db1.capacity != db2.capacity {
		t.Error("differenc counters")
	}
	_, _, err = db2.getById(id2)
	if err != nil {
		t.Error("error getting item ", id2, err)
	}
	pers, err := db2.Get([]byte("Person1"))
	if err != nil {
		t.Error("error getting by key", err)

	}
	if pers.Age != testData[id1].Age {
		t.Error("Wrong data")
	}
	_, _, err = db2.getById(9999)
	if err == nil {
		t.Error("got non existing object")
	}

	if hr := db2.cache.GetHitRate(); hr != 0 {
		log.Infof("Cache hit rate %.2f", hr)
		t.Error("wrong hit rate")
	}
	_, _, _ = db2.getById(id2)

	if hr := db2.cache.GetHitRate(); hr < 33.32 || hr > 33.34 {
		log.Infof("Cache hit rate %f", hr)
		t.Error("wrong hit rate")
	}
	_, _, _ = db1.getById(id2)

	if hr := db1.cache.GetHitRate(); hr != 100 {
		log.Infof("Cache hit rate %f", hr)
		t.Error("wrong hit rate")
	}

	// test update
	pers, err = db2.Get([]byte("Person1"))
	if err != nil {
		t.Error("error getting by key", err)
	}
	pers.Age = 123
	id1, err = db2.Update([]byte("Person1"), pers)
	if err != nil {
		t.Error("update failed")
	}
	key, val, err := db2.getById(id1)
	if err != nil {
		t.Error("problem getting updated")
	}
	if string(key) != "Person1" {
		t.Error("Wrong key")
	}
	if val.Age != 123 {
		t.Error("Wrong value")
	}
	db1.Close()
	db2.Close()
}

func TestCache(t *testing.T) {
	var (
		d   *benchmarkData
		err error
	)

	db, _ := Open[benchmarkData]("benchmark")
	Destroy(db, "benchmark")
	db, _ = Open[benchmarkData]("benchmark")

	// gen 2 x times the cache capacity
	// so the expected hitrate is 50%
	var numElements = 2 * CacheMaxItems

	reference := make(map[ID]string)
	for n := 0; n < numElements; n++ {
		d = NewBenchmarkData(n)
		db.Append([]byte(fmt.Sprintf("Item%d", n)), d)
		reference[ID(n)] = d.Str
	}
	db.Close()

	db, _ = Open[benchmarkData]("benchmark")
	for n := 0; n < numElements; n++ {
		rndNo := ID(rand.Intn(numElements))
		if _, d, err = db.getById(rndNo); err != nil {
			t.Error("get failed")
		}
		if d.Str != reference[ID(rndNo)] {
			t.Error("values dont match", rndNo)
		}
	}
	log.Info("Cache Hit rate: ", db.GetHitRate(), " %")
}

func TestDeleteLogic(t *testing.T) {
	var (
		value *benchmarkData
		err   error
	)
	const N = CacheMaxItems * 2
	log.Info("Testing N = ", N)
	seq := genRandomSequence(N) // shuffle Item IDs to delete them randomly
	hashes := make([]uint32, N)

	db, _ := Open[benchmarkData]("benchmark")
	Destroy(db, "benchmark")

	db, _ = Open[benchmarkData]("benchmark")
	// add
	for n := 0; n < N; n++ {
		value = NewBenchmarkData(n)
		key := []byte(fmt.Sprintf("Item%d", n))
		db.Append(key, value)
		hashes[n] = getHash(key)
	}
	// delete randomly
	for n := 0; n < N; n++ {
		err = db.deleteById(ID(seq[n]), hashes[seq[n]])
		if err != nil {
			t.Error("should be able to delete")
		}
	}
	// delete last on
	err = db.deleteById(0, hashes[0])
	if err == nil {
		t.Error("should not be able to delete")
	}

	// everything should be deleted now
	l := len(db.markedForDelete)
	if l != N {
		t.Error("there should be ", N, " deleted")
	}
	l = len(db.cache.queue)
	if l != 0 {
		t.Error("cache should be empty, but is :", l)
	}
	if db.Close() != nil {
		t.Error("error closing db :", err)
	}
}
