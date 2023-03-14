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

func BenchmarkCache(b *testing.B) {
	var (
		d   *benchmarkData
		err error
	)
	const CacheSize = 100
	const numElements = 200

	Destroy("benchmark")
	db, _ := NewDb[benchmarkData]("benchmark", CacheSize)

	reference := make(map[ID]string)
	for n := 0; n < numElements; n++ {
		d = NewBenchmarkData(n)
		db.Append([]byte(fmt.Sprintf("Item%d", n)), d)
		reference[ID(n)] = d.Str
	}
	db.Close()

	db, _ = NewDb[benchmarkData]("benchmark", CacheSize)
	for n := 0; n < b.N; n++ {
		rndNo := ID(rand.Intn(numElements))
		if _, _, err = db.getById(rndNo); err != nil {
			b.Error("get failed")
		}
	}
	log.Info("-> ", b.N, " iterations. Cache Hit rate: ", db.cache.GetHitRate(), " %")
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
