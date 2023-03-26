package simpledb

import (
	"bytes"
	"encoding/binary"
	"os"
	"unsafe"

	"github.com/kkonat/simpledb/hash"
)

type blockHeader struct {
	Length  uint32 // uppercase, because must be exportable for binary encoding
	Id      ID
	KeyHash hash.Type
	KeyLen  uint32 // can not be uint16, data is 32-bit word-aligned anyway, sizeof will return untrue no. of bytes
	DataLen uint32 // can not be uint
}

func (b *blockHeader) read(file *os.File) (err error) {
	return binary.Read(file, binary.LittleEndian, b)
}
func blockheadersSize() int {
	return int(unsafe.Sizeof(blockHeader{}))
}

func (b *blockHeader) getBytes() (header []byte) {
	buff := bytes.NewBuffer(header)
	binary.Write(buff, binary.LittleEndian, b)
	return buff.Bytes()
}

type block struct {
	blockHeader
	key   string
	value []byte
}

func NewBlock(id ID, key string, value []byte) *block {
	var header blockHeader
	headerLen := blockheadersSize()
	blockLen := headerLen + len(key) + len(value)
	header = blockHeader{
		Id:      id,
		KeyHash: hash.Get(key),
		KeyLen:  uint32(len(key)),
		DataLen: uint32(len(value)),
		Length:  uint32(blockLen),
	}
	block := &block{blockHeader: header, key: key, value: value}
	return block
}

func (b *block) getBytes() []byte {
	headerBytes := b.blockHeader.getBytes()
	blockBytes := append(headerBytes, b.key...)
	blockBytes = append(blockBytes, b.value...)
	return blockBytes
}

func (b *block) setBytes(blockBytes []byte) {
	buff := bytes.NewBuffer(blockBytes)

	binary.Read(buff, binary.LittleEndian, &(b.blockHeader))
	keyStart := int(unsafe.Sizeof(blockHeader{}))
	valueStart := keyStart + int(b.blockHeader.KeyLen)
	b.key = string(blockBytes[keyStart:valueStart])
	b.value = blockBytes[valueStart:]
}
