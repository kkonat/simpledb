package simpledb

import (
	"fmt"
	"math/rand"
	"testing"

	log "github.com/sirupsen/logrus"
)

func TestNew(t *testing.T) {
	d, err := Open[Person]("testdb", 1)
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
	{"Eugen", "von Kotke", 123},
}

func TestDestroy(t *testing.T) {
	const CacheSize = 1
	db, err := Open[Person]("testDestroy", CacheSize)
	if err != nil {
		t.Errorf("failed to create database: %v", err)
	}
	db.Append("Person1", &testData[0])
	db.Close()
	err = db.Destroy()
	if err != nil {
		t.Errorf("failed to kill database: %v", err)
	}
}
func TestAppendGetWithCache(t *testing.T) {
	const CacheSize = 1
	DeleteDbFile("testAppendGet")

	// add item to db, then close (flush to disK)
	db, err := Open[Person]("testAppendGet", CacheSize)
	if err != nil {
		t.Errorf("failed to create database: %v", err)
	}
	db.Append("Person1", &testData[0])
	db.Close()

	// check if write cache flushed ok, item persisted
	db, err = Open[Person]("testAppendGet", CacheSize)
	if err != nil {
		t.Errorf("failed to reopen database: %v", err)
	}
	item, err := db.Get("Person1")
	if err != nil {
		t.Error("failed to get item", err)
	}
	if *item != testData[0] {
		t.Error("data mismatch")
	}

	// add a new item
	db.Append("Person2", &testData[1])

	// check if it can be retrieved straight from the cache
	item, err = db.Get("Person2")
	if err != nil {
		t.Error("failed to get item", err)
	}
	if *item != testData[1] {
		t.Error("data mismatch")
	}

	// check if deleted ok from write cache
	db.Delete("Person2")
	_, err = db.Get("Person2")
	if err == nil {
		t.Error("should not get", err)
	}
	// check if can be read from disk or read cache
	item, err = db.Get("Person1")
	if err != nil {
		t.Error("failed to get item", err)
	}
	if *item != testData[0] {
		t.Error("data mismatch")
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

	id1, err := db1.Append("Person1", &testData[0])
	if err != nil {
		t.Errorf("Append fail")
	}
	if id1 != 0 {
		t.Error("Bad id")
	}
	id2, err := db1.Append("Person2", &testData[1])
	if err != nil {
		t.Errorf("Append fail")
	}
	if id2 != 1 {
		t.Error("Bad id")
	}
	db1.Close()
	db1, _ = Open[Person]("testBasicFunx", CacheSize)
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
	_, _, err = db2.getItem(id2)
	if err != nil {
		t.Error("error getting item ", id2, err)
	}
	// test get the same key
	pers, err := db2.Get("Person1")
	if err != nil {
		t.Error("error getting by key", err)
	}
	if pers.Age != testData[id1].Age {
		t.Error("Wrong data")
	}
	// this should not exist
	_, _, err = db2.getItem(9999)
	if err == nil {
		t.Error("got non existing object")
	}
	_, err = db2.Get("9999")
	if err == nil {
		t.Error("got non existing object")
	}

	// test chache
	if hr := db2.readCache.GetHitRate(); hr != 0 {
		log.Infof("Cache hit rate %.2f", hr)
		t.Error("wrong hit rate")
	}
	_, _, _ = db2.getItem(id2)

	if hr := db2.readCache.GetHitRate(); hr < 24.99 || hr > 25.01 {
		log.Infof("Cache hit rate %f", hr)
		t.Error("wrong hit rate")
	}
	_, _, _ = db1.getItem(id2)
	_, _, _ = db1.getItem(id2)
	if hr := db1.readCache.GetHitRate(); hr < 49.99 || hr > 50.01 {
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
	db.Append("Person1", &testData[0])
	db.Append("Person2", &testData[1])
	db.Append("Person3", &Person{Name: "Rudolfshien", Surname: "Von Der Shuster", Age: 4})
	db.Close()
	// test update
	db, _ = Open[Person]("testUpdate2", CacheSize)
	pers, _ := db.Get("Person2")
	pers.Age = 1234
	db.Update("Person2", pers)

	db.Delete("Person1")

	pers, _ = db.Get("Person3")
	pers.Name = "Rudolf"
	pers.Age = 12
	db.Update("Person3", pers)
	db.Close()

	db, _ = Open[Person]("testUpdate2", CacheSize)

	val, _ := db.Get("Person3")

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
	const CacheSize = 1000
	DeleteDbFile("benchmarkCache")
	db, _ := Open[benchmarkData]("benchmarkCache", CacheSize)

	// gen 2 x times the cache capacity
	// so the expected hitrate is 50%
	var numElements = int(2 * CacheSize)
	var expectedHitrate = 100. * float64(CacheSize) / float64(numElements)

	reference := make(map[ID]string)
	for n := 0; n < numElements; n++ {
		d = NewBenchmarkData(n)
		//d = &benchmarkData{Value: uint(n), Str: fmt.Sprintf("Item %d", n)}
		db.Append(fmt.Sprintf("Item%d", n), d)
		reference[ID(n)] = d.Str
	}
	db.Close()

	db, _ = Open[benchmarkData]("benchmarkCache", CacheSize)
	// fill cache
	for n := 0; n < CacheSize; n++ {
		db.getItem(ID(n))
	}
	db.readCache.statistics.requests = 0 // artificially reset no. of requests
	// test hits
	for n := 0; n < numElements; n++ {

		if _, d, err = db.getItem(ID(n)); err != nil {
			t.Error("get failed")
		}
		if d.Str != reference[ID(n)] {
			t.Errorf("values dont match: %s vs %s", d.Str, reference[ID(n)])
		}
	}
	hr := db.readCache.GetHitRate()
	if hr < expectedHitrate-5 || hr > expectedHitrate+5 {
		t.Error("Wrong cache Hit rate: ", hr, "%, expected:", expectedHitrate, "%")
	}
}

func TestDeleteAndUpdate(t *testing.T) {
	var (
		value *benchmarkData
		err   error
	)
	const CacheSize = 1000
	const N = 2000

	elements := make([]int, N)

	DeleteDbFile("delLogic")

	db, _ := Open[benchmarkData]("delLogic", CacheSize)

	// add
	for n := 0; n < N; n++ {
		value = NewBenchmarkData(n)
		key := fmt.Sprintf("Item%d", n)
		elements[n] = n
		db.Append(key, value)
	}
	db.Close()

	// modify half
	db, _ = Open[benchmarkData]("delLogic", CacheSize)
	for n := N / 2; n < N; n++ {
		x := rand.Intn(N)
		key := fmt.Sprintf("Item%d", x)
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
		key := fmt.Sprintf("Item%d", elNo)
		err = db.Delete(key)
		if err != nil {
			t.Errorf("el:%d, key:%s", elNo, string(key))
			t.Error(fmt.Errorf("err delete %w", err))
			return
		}
		elements[which] = elements[len(elements)-1]
		elements = elements[:len(elements)-1]
	}

	err = db.Delete("Item0")
	if err == nil {
		t.Error("should not be able to delete")
	}

	l := db.readCache.queue.Len()
	if l != 0 {
		t.Error("cache should be empty, but is :", l)
	}
	if err = db.Close(); err != nil {
		t.Error("error closing db :", err)
	}
	log.Info("Cache Hit rate: ", db.readCache.GetHitRate(), " %")
	db.Close()
}
