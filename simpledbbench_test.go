package simpledb

import (
	"fmt"
	"math/rand"
	"testing"

	log "github.com/sirupsen/logrus"
)

type benchmarkData struct {
	Value uint
	Str   string
}

func NewBenchmarkData(n int) *benchmarkData {
	d := &benchmarkData{Value: uint(n)}
	d.Str = getRandomLetters(16)
	return d
}
func BenchmarkPerfWithCache(b *testing.B) {
	var (
		d   *benchmarkData
		err error
	)
	var CacheSize = uint32(b.N)
	var numElements = uint32(b.N)

	DeleteDbFile("benchmark")
	db, _ := Open[benchmarkData]("benchmark", CacheSize)

	reference := make(map[ID]string)
	for n := 0; n < b.N; n++ {
		d = NewBenchmarkData(n)
		db.Append(fmt.Sprintf("Item%d", n), d)
		reference[ID(n)] = d.Str
	}
	// db.Close()

	// db, _ = Open[benchmarkData]("benchmark", CacheSize)
	for n := 0; n < b.N; n++ {
		rndNo := ID(rand.Intn(int(numElements)))
		if _, _, err = db.getItem(rndNo); err != nil {
			b.Error("get failed")
		}
	}
	db.Close()
	log.Info("-> ", b.N, " iterations. Cache Hit rate: ", db.readCache.GetHitRate(), " %")
}

func BenchmarkDeleteAndUpdate(b *testing.B) {
	var (
		value *benchmarkData
		err   error
	)
	//var N = 600
	var N = b.N
	var CacheSize = 1 + uint32(N/2)
	log.Info("N=", N)

	elements := make([]int, N)

	DeleteDbFile("delLogic")

	db, _ := Open[benchmarkData]("delLogic", CacheSize)

	// add

	for n := 0; n < N; n++ {
		value = &benchmarkData{Str: fmt.Sprintf("value%d ", n)}
		key := fmt.Sprintf("Item%d", n)
		elements[n] = n
		db.Append(key, value)

	}
	db.Close()

	// modify half
	db, _ = Open[benchmarkData]("delLogic", CacheSize)
	for n := 0; n < N/2; n++ {
		x := rand.Intn(N)
		key := fmt.Sprintf("Item%d", x)

		value, err := db.Get(key)
		if err != nil {
			db.Get(key)
			b.Error("n=", n, "should be able to get :", string(key))
			return
		}
		// log.Info(x, ":", string(key), ":", value.Str)
		value.Value = 0
		value.Str += " mod"
		_, err = db.Update(key, value)
		if err != nil {
			b.Error("fail")
		}
		// fmt.Print(x, "->", id, "\t")
	}
	db.Close()

	// delete randomly
	db, _ = Open[benchmarkData]("delLogic", CacheSize)
	for n := 0; n < N; n++ {
		which := rand.Intn(len(elements))
		elNo := elements[which]
		key := fmt.Sprintf("Item%d", elNo)
		err = db.Delete(key)
		if err != nil {
			b.Errorf("el:%d, key:%s", elNo, string(key))
			b.Error(fmt.Errorf("err delete %w", err))
			return
		}
		elements[which] = elements[len(elements)-1]
		elements = elements[:len(elements)-1]
	}
	db.Close()
}
