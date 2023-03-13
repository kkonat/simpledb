package simpledb

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

type blockHeader struct {
	Length    uint32 // uppercase, because must be exportable for binary encoding
	Id        uint32
	Timestamp uint64
	KeyHash   uint32
	KeyLen    uint32 // can not be uint16, data is 32-bit word-aligned anyway, sizeof will return untrue no. of bytes
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
		Id:        uint32(id),
		Timestamp: timestamp,
		KeyHash:   getHash(key),
		KeyLen:    uint32(len(key)),
		Length:    uint32(blockLen),
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
	b.key = blockBytes[keyStart:valueStart]
	b.value = blockBytes[valueStart:]
}
