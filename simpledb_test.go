package simpledb

import (
	"fmt"
	"math/rand"
	"testing"

	log "github.com/sirupsen/logrus"
)

func TestNew(t *testing.T) {
	d, err := Open[Person]("testdb", 0)
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
	db, err := Open[Person]("testDestroy", CacheSize)
	if err != nil {
		t.Errorf("failed to create database: %v", err)
	}
	db.Append([]byte("Person1"), &testData[0])
	db.Close()
	err = db.Destroy()
	if err != nil {
		t.Errorf("failed to kill database: %v", err)
	}
}
func TestBasicFunctionality(t *testing.T) {
	const CacheSize = 100

	var err error
	DeleteDbFile("testBasicFunx")

	db1, err := Open[Person]("testBasicFunx", CacheSize)
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

	// open another db from the same file
	db2, err := Open[Person]("testBasicFunx", CacheSize)
	if err != nil {
		t.Errorf("failed to open database: %v", err)
	}
	// test internals
	if db1.currentOffset != db2.currentOffset {
		t.Error("different offsets")
	}
	if db1.ItemsCount != db2.ItemsCount {
		t.Error("differenc counters")
	}
	// test get the same item
	_, _, err = db2.getById(id2)
	if err != nil {
		t.Error("error getting item ", id2, err)
	}
	// test get the same key
	pers, err := db2.Get([]byte("Person1"))
	if err != nil {
		t.Error("error getting by key", err)
	}
	if pers.Age != testData[id1].Age {
		t.Error("Wrong data")
	}
	// this should not exist
	_, _, err = db2.getById(9999)
	if err == nil {
		t.Error("got non existing object")
	}
	_, err = db2.Get([]byte("9999"))
	if err == nil {
		t.Error("got non existing object")
	}

	// test chache
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
	db1.Close()
	db2.Close()
}

func TestUpdate2(t *testing.T) {
	const CacheSize = 100
	DeleteDbFile("testUpdate2")
	db, _ := Open[Person]("testUpdate2", CacheSize)
	db.Append([]byte("Person1"), &testData[0])
	db.Append([]byte("Person2"), &testData[1])
	db.Append([]byte("Person3"), &Person{Name: "Rudolfshien", Surname: "Von Der Shuster", Age: 4})
	db.Close()
	// test update
	db, _ = Open[Person]("testUpdate2", CacheSize)
	pers, _ := db.Get([]byte("Person2"))
	pers.Age = 1234
	db.Update([]byte("Person2"), pers)

	db.Delete([]byte("Person1"))

	pers, _ = db.Get([]byte("Person3"))
	pers.Name = "Rudolf"
	pers.Age = 12
	db.Update([]byte("Person3"), pers)
	db.Close()

	db, _ = Open[Person]("testUpdate2", CacheSize)

	val, _ := db.Get([]byte("Person3"))

	if val.Age != 12 && val.Name != "Rudolf" {
		t.Error("Wrong value")
	}
	db.Close()
}

func TestCache(t *testing.T) {
	var (
		d   *benchmarkData
		err error
	)
	const CacheSize = 100

	DeleteDbFile("benchmarkCache")
	db, _ := Open[benchmarkData]("benchmarkCache", CacheSize)

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

	db, _ = Open[benchmarkData]("benchmarkCache", CacheSize)
	for n := 0; n < numElements; n++ {
		rndNo := ID(rand.Intn(numElements))
		log.Info(n)
		if _, d, err = db.getById(rndNo); err != nil {
			t.Error("get failed")
		}
		if d.Str != reference[ID(rndNo)] {
			t.Error("values dont match", rndNo)
		}
	}
	log.Info("Cache Hit rate: ", db.cache.GetHitRate(), " %")
}

func TestDeleteAndUpdate(t *testing.T) {
	var (
		value *benchmarkData
		err   error
	)
	const CacheSize = 100
	const N = 200

	elements := make([]int, N)

	DeleteDbFile("delLogic")

	db, _ := Open[benchmarkData]("delLogic", CacheSize)

	// add
	for n := 0; n < N; n++ {
		value = NewBenchmarkData(n)
		key := []byte(fmt.Sprintf("Item%d", n))
		elements[n] = n
		db.Append(key, value)
	}
	db.Close()

	// modify half
	db, _ = Open[benchmarkData]("delLogic", CacheSize)
	for n := N / 2; n < N; n++ {
		x := rand.Intn(N)
		key := []byte(fmt.Sprintf("Item%d", x))
		value, err := db.Get(key)
		if err != nil {
			t.Error("should be able to get")
		}
		value.Str += " mod"
		value.Value = 0
		db.Update(key, value)
		if err != nil {
			t.Error("should be able to update")
		}
	}
	db.Close()

	// delete randomly
	db, _ = Open[benchmarkData]("delLogic", CacheSize)
	log.Info("db size", db.ItemsCount)
	for n := 0; n < N; n++ {
		which := rand.Intn(len(elements))
		elNo := elements[which]
		key := []byte(fmt.Sprintf("Item%d", elNo))
		err = db.Delete(key)
		if err != nil {
			t.Errorf("el:%d, key:%s", elNo, string(key))
			t.Error(fmt.Errorf("err delete %w", err))
			return
		}
		elements[which] = elements[len(elements)-1]
		elements = elements[:len(elements)-1]
	}

	err = db.Delete([]byte("Item0"))
	if err == nil {
		t.Error("should not be able to delete")
	}

	l := db.cache.queue.Len()
	if l != 0 {
		t.Error("cache should be empty, but is :", l)
	}
	if err = db.Close(); err != nil {
		t.Error("error closing db :", err)
	}
	log.Info("Cache Hit rate: ", db.cache.GetHitRate(), " %")
	db.Close()
}
