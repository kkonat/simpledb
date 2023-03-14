package simpledb

import (
	"fmt"
	"math/rand"
	"testing"

	log "github.com/sirupsen/logrus"
)

func TestNew(t *testing.T) {
	d, err := NewDb[Person]("testdb", 0)
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

func TestDestroy(t *testing.T) {
	const CacheSize = 0
	db, err := NewDb[Person]("testdb", CacheSize)
	if err != nil {
		t.Errorf("failed to create database: %v", err)
	}
	db.Append([]byte("Person1"), &testData[0])
	db.Close()
	err = Destroy("db\\testdb.sdb")
	if err != nil {
		t.Errorf("failed to kill database: %v", err)
	}
}
func TestBasicFunctionality(t *testing.T) {
	const CacheSize = 100
	Destroy("db\\testdb.sdb")

	var err error
	db1, err := NewDb[Person]("testdb", CacheSize)
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

	db2, err := NewDb[Person]("testdb", CacheSize)
	if err != nil {
		t.Errorf("failed to open database: %v", err)
	}
	if db1.currentOffset != db2.currentOffset {
		t.Error("different offsets")
	}
	if db1.ItemsCount != db2.ItemsCount {
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
	err = db2.Update([]byte("Person1"), pers)
	if err != nil {
		t.Error("update failed")
	}
	val, err := db2.Get([]byte("Person1"))
	if err != nil {
		t.Error("problem getting updated")
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
	const CacheSize = 100

	Destroy("db\\benchmark.sdb")
	db, _ := NewDb[benchmarkData]("benchmark", CacheSize)

	// gen 2 x times the cache capacity
	// so the expected hitrate is 50%
	var numElements = int(2 * CacheSize)

	reference := make(map[ID]string)
	for n := 0; n < numElements; n++ {
		d = NewBenchmarkData(n)
		db.Append([]byte(fmt.Sprintf("Item%d", n)), d)
		reference[ID(n)] = d.Str
	}
	db.Close()

	db, _ = NewDb[benchmarkData]("benchmark", CacheSize)
	for n := 0; n < numElements; n++ {
		rndNo := ID(rand.Intn(numElements))
		if _, d, err = db.getById(rndNo); err != nil {
			t.Error("get failed")
		}
		if d.Str != reference[ID(rndNo)] {
			t.Error("values dont match", rndNo)
		}
	}
	log.Info("Cache Hit rate: ", db.cache.GetHitRate(), " %")
}

func TestDeleteLogic(t *testing.T) {
	var (
		value *benchmarkData
		err   error
	)
	const CacheSize = 100
	const N = 200

	log.Info("Testing N = ", N)
	seq := genRandomSequence(N) // shuffle Item IDs to delete them randomly
	hashes := make([]Hash, N)

	Destroy("benchmark")

	db, _ := NewDb[benchmarkData]("benchmark", CacheSize)

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
	log.Info("cache len:", len(db.cache.queue))
	err = db.deleteById(0, hashes[0])
	if err == nil {
		t.Error("should not be able to delete")
	}

	// everything should be deleted now
	l := len(db.deleted)
	if l != N {
		t.Error("there should be ", N, " deleted")
	}
	log.Info("cache len:", len(db.cache.queue))
	l = len(db.cache.queue) /// TODO: If tests are executed concurrently it sometimes fails
	if l != 0 {
		t.Error("cache should be empty, but is :", l)
	}
	if err = db.Close(); err != nil {
		t.Error("error closing db :", err)
	}
}
