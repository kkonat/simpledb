package simpledb

import (
	"math/rand"
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

	id1, err := db1.Append(&testData[0])
	if err != nil {
		t.Errorf("Append fail")
	}
	if id1 != 0 {
		t.Error("Bad id")
	}
	id2, err := db1.Append(&testData[1])
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
	if db1.currOffset != db2.currOffset {
		t.Error("different offsets")
	}
	if db1.itemCounter != db2.itemCounter {
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

	if hr := db2.Cache.hitRate(); hr != 0 {
		log.Infof("Cache hit rate %.2f", hr)
		t.Error("wrong hit rate")
	}
	_, _ = db2.Get(id2)

	if hr := db2.Cache.hitRate(); hr < 50 || hr > 50 {
		log.Infof("Cache hit rate %f", hr)
		t.Error("wrong hit rate")
	}
	_, _ = db1.Get(id2)

	if hr := db1.Cache.hitRate(); hr != 100 {
		log.Infof("Cache hit rate %f", hr)
		t.Error("wrong hit rate")
	}
}

type benchmarkData struct {
	Value uint
	Str   string
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func NewBenchmarkData(n int) *benchmarkData {
	d := &benchmarkData{Value: uint(n)}
	b := make([]rune, 16)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	d.Str = string(b)
	return d
}

func TestCache(t *testing.T) {
	var (
		d   *benchmarkData
		err error
	)

	db, _ := Connect[benchmarkData]("benchmark")
	db.Kill("benchmark")
	db, _ = Connect[benchmarkData]("benchmark")

	// gen 2 x times the cache capacity
	// so the expected hitrate is 50%
	var numElements = 2 * MemCacheMaxItems

	reference := make(map[DbItemID]string)
	for n := 0; n < numElements; n++ {
		d = NewBenchmarkData(n)
		db.Append(d)
		reference[DbItemID(n)] = d.Str
	}
	db.Close()

	db, _ = Connect[benchmarkData]("benchmark")
	for n := 0; n < numElements; n++ {
		rndNo := DbItemID(rand.Intn(numElements))
		if d, err = db.Get(rndNo); err != nil {
			t.Error("get failed")
		}
		if d.Str != reference[DbItemID(rndNo)] {
			t.Error("values dont match", rndNo)
		}
	}
	log.Info("Cache Hit rate: ", db.hitRate(), " %")
}
func TestBuildoffsets(t *testing.T) {
	db, _ := Connect[benchmarkData]("benchmark")
	db.Close()
}
func BenchmarkCache(b *testing.B) {
	var (
		d   *benchmarkData
		err error
	)

	db, _ := Connect[benchmarkData]("benchmark")
	db.Kill("benchmark")
	db, _ = Connect[benchmarkData]("benchmark")

	var numElements = int(1.33 * MemCacheMaxItems)

	reference := make(map[DbItemID]string)
	for n := 0; n < numElements; n++ {
		d = NewBenchmarkData(n)
		db.Append(d)
		reference[DbItemID(n)] = d.Str
	}
	db.Close()

	db, _ = Connect[benchmarkData]("benchmark")
	for n := 0; n < b.N; n++ {
		rndNo := DbItemID(rand.Intn(numElements))
		if _, err = db.Get(rndNo); err != nil {
			b.Error("get failed")
		}
	}
	log.Info("-> ", b.N, " iterations. Cache Hit rate: ", db.hitRate(), " %")
}
