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
func BenchmarkCache(b *testing.B) {
	var (
		d   *benchmarkData
		err error
	)
	var CacheSize = uint32(b.N / 2)
	var numElements = uint32(b.N)

	db, _ := Open[benchmarkData]("benchmark", CacheSize)
	db.Destroy()
	db, _ = Open[benchmarkData]("benchmark", CacheSize)

	reference := make(map[ID]string)
	for n := 0; n < b.N; n++ {
		d = NewBenchmarkData(n)
		db.Append([]byte(fmt.Sprintf("Item%d", n)), d)
		reference[ID(n)] = d.Str
	}
	db.Close()

	db, _ = Open[benchmarkData]("benchmark", CacheSize)
	for n := 0; n < b.N; n++ {
		rndNo := ID(rand.Intn(int(numElements)))
		if _, _, err = db.getById(rndNo); err != nil {
			b.Error("get failed")
		}
	}
	log.Info("-> ", b.N, " iterations. Cache Hit rate: ", db.cache.GetHitRate(), " %")
}
