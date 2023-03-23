package simpledb

import "os"

// This is a sort-of a bufio.Writer, but it performs  specific work on buffer flush, which bufio can't
// it. the database must know what is still in the buffer and has not been saved to disk

type buffItem struct {
	data    []byte
	deleted bool
}
type writeBuff struct {
	buffered    map[ID]*buffItem
	addedIDs    []ID
	accumulated uint64
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
	b.accumulated += uint64(len(data))
}

func (b *writeBuff) remove(id ID) {
	b.buffered[id].deleted = true
}
func (b *writeBuff) has(id ID) bool {
	_, buffered := b.buffered[id]
	return buffered && !b.buffered[id].deleted
}

func (b *writeBuff) reset() {
	b.buffered = make(map[ID]*buffItem)
	b.addedIDs = make([]ID, 0)
	b.accumulated = 0
}

func (b *writeBuff) size() uint64 {
	return b.accumulated
}

type idOffset struct {
	id     ID
	offset uint64
}

func (b *writeBuff) flush(file *os.File) (bo []idOffset, err error) {
	currentOffset := uint64(0)
	for _, id := range b.addedIDs {
		bi := b.buffered[id]
		if !bi.deleted {
			if _, err = file.Write(bi.data); err != nil {
				return
			}
			bo = append(bo, idOffset{id, currentOffset})
			currentOffset += uint64(len(bi.data))
		}
	}
	b.reset()
	return
}
