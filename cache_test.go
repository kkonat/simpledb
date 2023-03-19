package simpledb

import (
	"math/rand"
	"testing"

	log "github.com/sirupsen/logrus"
)

func BenchmarkCache(b *testing.B) {
	b.StopTimer()
	var (
		d *benchmarkData
	)
	var CacheSize = uint32(b.N)
	var numElements = uint32(b.N)

	cache := newCache[benchmarkData](CacheSize)

	reference := make(map[ID]string)
	for n := 0; n < b.N; n++ {
		d = NewBenchmarkData(n)
		cache.add(&Item[benchmarkData]{ID: ID(n), Value: d})
		reference[ID(n)] = d.Str
	}
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		rndNo := ID(rand.Intn(int(numElements)))
		cache.checkAndGet(rndNo)

		// rndNo = ID(rand.Intn(int(numElements)))
		// cache.touch(rndNo)
	}

	log.Info("-> ", b.N, " iterations. Cache Hit rate: ", cache.GetHitRate(), " %")
}
