package simpledb

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

type blockHeader struct {
	Offset    uint32 // 4
	ID        uint32 // 8
	Timestamp uint64 // 16
	KeyHash   uint32 // 20
	KeyLen    uint32 // can not be 2, data is 32-bit word-aligned
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
	headerlen := int(unsafe.Sizeof(blockHeader{}))
	blocklen := headerlen + len(key) + len(value)
	header := blockHeader{
		ID:        uint32(id),
		Timestamp: timestamp,
		KeyHash:   getHash(key),
		KeyLen:    uint32(len(key)),
		Offset:    uint32(blocklen),
	}
	block := &block{blockHeader: header, key: key, value: value}
	return block
}

func (b *block) getBytes() []byte {

	headerB := b.blockHeader.getBytes()
	blockB := append(headerB, b.key...)
	blockB = append(blockB, b.value...)

	return blockB
}

func (b *block) setBytes(blockBytes []byte) {
	var h blockHeader
	keyStart := int(unsafe.Sizeof(h))

	buff := bytes.NewBuffer(blockBytes)
	binary.Read(buff, binary.LittleEndian, &(b.blockHeader))
	valueStart := keyStart + int(b.blockHeader.KeyLen)

	b.key = blockBytes[keyStart:valueStart]
	b.value = blockBytes[valueStart:]
}
