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
	_, _, err = db2.GetById(id2)
	if err != nil {
		t.Error("error getting item ", id2, err)
	}
	pers, err := db2.GetByKey([]byte("Person1"))
	if err != nil {
		t.Error("error getting by key", err)

	}
	if pers.Age != testData[id1].Age {
		t.Error("Wrong data")
	}
	_, _, err = db2.GetById(9999)
	if err == nil {
		t.Error("got non existing object")
	}

	if hr := db2.Cache.getHitRate(); hr != 0 {
		log.Infof("Cache hit rate %.2f", hr)
		t.Error("wrong hit rate")
	}
	_, _, _ = db2.GetById(id2)

	if hr := db2.Cache.getHitRate(); hr < 33.32 || hr > 33.34 {
		log.Infof("Cache hit rate %f", hr)
		t.Error("wrong hit rate")
	}
	_, _, _ = db1.GetById(id2)

	if hr := db1.Cache.getHitRate(); hr != 100 {
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
		if _, d, err = db.GetById(rndNo); err != nil {
			t.Error("get failed")
		}
		if d.Str != reference[ID(rndNo)] {
			t.Error("values dont match", rndNo)
		}
	}
	log.Info("Cache Hit rate: ", db.getHitRate(), " %")
}

func genRandomSequence(N int) []int {

	var seq = make([]int, N)

	for i := 1; i < N; i++ {

		p := 1 + rand.Intn(N-1)

		if seq[p] == 0 {
			seq[p] = i
		} else {
			for ; p < N && seq[p] != 0; p++ {
			}
			if p < N {
				seq[p] = i
			} else {
				for p = 1; p < N && seq[p] != 0; p++ {
				}
				if p < N {
					seq[p] = i
				} else {
					panic("No place left")
				}
			}
		}
	}
	return seq
}

func TestDeleteLogic(t *testing.T) {
	var (
		d   *benchmarkData
		err error
	)
	const N = CacheMaxItems * 2
	log.Info("Testing N = ", N)
	seq := genRandomSequence(N)
	db, _ := Open[benchmarkData]("benchmark")
	Destroy(db, "benchmark")
	db, _ = Open[benchmarkData]("benchmark")
	for n := 0; n < N; n++ {
		d = NewBenchmarkData(n)
		db.Append([]byte(fmt.Sprintf("Item%d", n)), d)
	}
	for n := 0; n < N; n++ {
		err = db.DeleteById(ID(seq[n]))
		if err != nil {
			t.Error("should be able to delete")
		}
	}
	err = db.DeleteById(0)
	if err == nil {
		t.Error("should not be able to delete")
	}
	l := len(db.markForDelete)
	if l != N {
		t.Error("there should be ", N, " deleted")
	}
	l = len(db.Cache.queue)
	if l != 0 {
		t.Error("cache should be empty, but is :", l)
	}
}
func BenchmarkCache(b *testing.B) {
	var (
		d   *benchmarkData
		err error
	)

	db, _ := Open[benchmarkData]("benchmark")
	Destroy(db, "benchmark")
	db, _ = Open[benchmarkData]("benchmark")

	var numElements = int(CacheMaxItems * 2)

	reference := make(map[ID]string)
	for n := 0; n < numElements; n++ {
		d = NewBenchmarkData(n)
		db.Append([]byte(fmt.Sprintf("Item%d", n)), d)
		reference[ID(n)] = d.Str
	}
	db.Close()

	db, _ = Open[benchmarkData]("benchmark")
	for n := 0; n < b.N; n++ {
		rndNo := ID(rand.Intn(numElements))
		if _, _, err = db.GetById(rndNo); err != nil {
			b.Error("get failed")
		}
	}
	log.Info("-> ", b.N, " iterations. Cache Hit rate: ", db.getHitRate(), " %")
}
