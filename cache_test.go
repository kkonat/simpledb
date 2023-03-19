package simpledb

import (
	"math/rand"
	"testing"
)

func BenchmarkCacheChkAndGet(b *testing.B) {
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
	}
}

func BenchmarkCacheTouch(b *testing.B) {
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
		cache.touch(rndNo)
	}
}

func BenchmarkCacheRemove(b *testing.B) {
	b.StopTimer()
	var (
		d *benchmarkData
	)
	var CacheSize = uint32(b.N)

	cache := newCache[benchmarkData](CacheSize)

	reference := []int{}
	for n := 0; n < b.N; n++ {
		d = NewBenchmarkData(n)
		cache.add(&Item[benchmarkData]{ID: ID(n), Value: d})
		reference = append(reference, n)
	}
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		left := len(reference)
		item := ID(rand.Intn(left))
		reference[item] = reference[left-1]
		reference = reference[:left-1]
		cache.removeItem(item)
	}
}
