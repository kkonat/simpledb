package simpledb

import (
	"math/rand"
	"testing"
)

func TestCacheFunctions(t *testing.T) {
	const N = 10000
	var CacheSize = N

	cache := newCache[benchmarkData](uint32(CacheSize))
	reference := make(map[ID]string)

	for n := 0; n < N; n++ {
		d := NewBenchmarkData(n)
		cache.add(&Item[benchmarkData]{ID: ID(n), Value: d})
		reference[ID(n)] = d.Str
	}

	for n := 0; n < N/100; n++ {
		cache.touch(ID(n))
	}

	for n := 0; n < len(reference); n++ {
		d, ok := cache.checkAndGet(ID(n))
		if !ok || reference[ID(n)] != d.Value.Str {
			t.Error("Data mismatch")
		}
	}

	for n := 0; n < N/2; n++ {
		cache.remove(ID(n))
		delete(reference, ID(n))
	}

	for id, str := range reference {
		cacheItem, ok := cache.checkAndGet(ID(id))
		if !ok || str != cacheItem.Value.Str {
			t.Error("Data mismatch")
		}
	}
}

func TestCacheHitRate(t *testing.T) {
	const N = 100
	var CacheSize = 0.5 * N
	var expectedHitrate = 100. * float64(CacheSize) / float64(N)

	cache := newCache[benchmarkData](uint32(CacheSize))
	reference := make(map[ID]string)

	for n := 0; n < N; n++ {
		d := NewBenchmarkData(n)
		cache.add(&Item[benchmarkData]{ID: ID(n), Value: d})
		reference[ID(n)] = d.Str
	}

	for n := 0; n < N/2; n++ {
		cache.checkAndGet(ID(rand.Intn(N)))
	}
	hr := cache.GetHitRate()
	if hr < expectedHitrate-5 || hr > expectedHitrate+5 {
		t.Error("Wrong cache Hit rate: ", hr, "%, expected:", expectedHitrate, "%")
	}
}

func BenchmarkCacheAdd(b *testing.B) {
	b.StopTimer()
	var (
		d *benchmarkData
	)
	var CacheSize = uint32(b.N/2) + 1
	//var CacheSize = uint32(b.N)

	cache := newCache[benchmarkData](CacheSize)

	reference := make(map[ID]string)
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		d = NewBenchmarkData(n)
		cache.add(&Item[benchmarkData]{ID: ID(n), Value: d})
		reference[ID(n)] = d.Str
	}
}

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
		which := rand.Intn(left)
		item := reference[which]
		if left >= 1 {
			reference[which] = reference[left-1]
			reference = reference[:left-1]
		}

		cache.remove(ID(item))
		// log.Info(item)
	}
}
