package simpledb

import (
	"os"
)

// This is a sort-of a bufio.Writer, but it performs  specific work on buffer flush, which bufio can't
// it. the database must know what is still in the buffer and has not been saved to disk

type buffItem struct {
	data    []byte
	deleted bool
}
type writeBuff struct {
	buffered    map[ID]*buffItem
	addedIDs    []ID
	accumulated int64
}

func newWriteBuff() *writeBuff {
	wb := &writeBuff{}
	wb.reset()
	return wb
}

func (b *writeBuff) append(id ID, data []byte) {
	bi := buffItem{
		data: data,
	}
	b.buffered[id] = &bi
	b.addedIDs = append(b.addedIDs, id)
	b.accumulated += int64(len(data))
}

func (b *writeBuff) remove(id ID) {
	b.buffered[id].deleted = true
}

func (b *writeBuff) contains(id ID) (exists bool) {
	_, buffered := b.buffered[id]
	return buffered && !b.buffered[id].deleted
}

func (b *writeBuff) reset() {
	b.buffered = make(map[ID]*buffItem)
	b.addedIDs = make([]ID, 0)
	b.accumulated = 0
}

type blockOffset struct {
	id     ID
	offset int64
}

func (b *writeBuff) flush(file *os.File) (bo []blockOffset, err error) {
	var written int
	var currentOffset int64

	for _, id := range b.addedIDs {
		bufItem := b.buffered[id]
		if !bufItem.deleted {
			if written, err = file.Write(bufItem.data); err != nil || written != len(bufItem.data) {
				return
			}
			bo = append(bo, blockOffset{id, currentOffset})
			currentOffset += int64(written)
		}
	}
	b.reset()
	return
}
