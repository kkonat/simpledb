package simpledb

import (
	"bytes"
	"encoding/binary"
	"os"
	"unsafe"

	"github.com/kkonat/simpledb/hash"
)

type blockHeader struct {
	Length    uint32 // uppercase, because must be exportable for binary encoding
	Id        ID
	Timestamp uint64
	KeyHash   hash.Type
	KeyLen    uint32 // can not be uint16, data is 32-bit word-aligned anyway, sizeof will return untrue no. of bytes
}

func (b *blockHeader) read(file *os.File) (err error) {
	return binary.Read(file, binary.LittleEndian, b)
}
func (b *blockHeader) getBytes() (header []byte) {
	buff := bytes.NewBuffer(header)
	binary.Write(buff, binary.LittleEndian, b)
	return buff.Bytes()
}

type block struct {
	blockHeader
	key   []byte
	value []byte
}

func NewBlock(id ID, timestamp uint64, key []byte, value []byte) *block {
	var header blockHeader
	headerLen := int(unsafe.Sizeof(header))
	blockLen := headerLen + len(key) + len(value)
	header = blockHeader{
		Id:        id,
		Timestamp: timestamp,
		KeyHash:   hash.Get(key),
		KeyLen:    uint32(len(key)),
		Length:    uint32(blockLen),
	}
	block := &block{blockHeader: header, key: key, value: value}
	return block
}
func (b *block) write(file *os.File) (bytesWritten int, err error) {
	return file.Write(b.getBytes())
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
	b.key = blockBytes[keyStart:valueStart]
	b.value = blockBytes[valueStart:]
}
