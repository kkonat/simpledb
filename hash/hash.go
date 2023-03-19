package hash

import (
	"hash/crc32"
)

var crc32table *crc32.Table

type Type uint32 // no need for larger hashes for now
var hashFunc func(data []byte) Type

func init() {
	crc32table = crc32.MakeTable(0x82f63b78)
	hashFunc = calcCrc32
}

func SetFunc(f func(data []byte) Type) {
	hashFunc = f
}
func Get(data []byte) Type {
	return hashFunc(data)
}

func calcCrc32(data []byte) Type {
	return Type(crc32.Checksum(data, crc32table))
}

func calcSimplesthash(data []byte) Type {
	var h Type
	for d := range data {
		h += h<<5 + h<<2 + h + Type(d)
	}
	return h
}
