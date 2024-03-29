package hash

import (
	"testing"
)

var vals = map[string]uint32{
	"too":  0x3ad11d33,
	"top":  0x78b5a877,
	"tor":  0xc09e2021,
	"tpp":  0x3058996d,
	"a000": 0x7552599f,
	"a001": 0x3cc1d896,
	"a002": 0xc6ff5c9b,
	"a003": 0xdcab7b0c,
	"a004": 0x780c7202,
	"a005": 0x7eb63e3a,
	"a006": 0x6b0a7a17,
	"a007": 0xcb5cb1ab,
	"a008": 0x5c2a15c0,
	"a009": 0x33339829,
	"a010": 0xeb1f336e,
	"a":    0x115ea782,
	"aa":   0x008ad357,
	"aaa":  0x7dfdc310,
}

func TestHash(t *testing.T) {
	SetFunc(calcSuperfasthash)
	// if Get("") != 0 {
	// 	t.Fail()
	// }
	// if Get("too") != 0x3ad11d33 {
	// 	t.Fail()
	// }
	// for k, v := range vals {
	// 	if Get(k) != Type(v) {
	// 		t.Error("Incorrect hash value")
	// 	}
	// }
	if Get("Item1") == Get("Item2") {
		t.Error("Problem with len(data) == 5")
	}
	if Get("Item001") == Get("Item002") {
		t.Error("Problem with len(data) == 7")
	}
	if Get("Item00001") == Get("Item00002") {
		t.Error("Problem with len(data) == 9")
	}
}

func BenchmarkSuperfastHash(b *testing.B) { // 11.21 ns/op
	SetFunc(calcSuperfasthash)
	for n := 0; n < b.N; n++ {
		Get("Testing hash function")
	}
}

func BenchmarkCRC32(b *testing.B) { // 32.09 ns/op
	SetFunc(calcCrc32)
	for n := 0; n < b.N; n++ {
		Get("Testing hash function")
	}
}

func BenchmarkSimplest(b *testing.B) { // 32.09 ns/op
	SetFunc(calcSimplesthash)
	for n := 0; n < b.N; n++ {
		Get("Testing hash function")
	}
}
