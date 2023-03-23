package simpledb

import "os"

// This is a sort-of bufio.Writer, but performing specific work on buffer flush
// it. the database must know what is still in the buffer and has not been saved to disk

type buffItem struct {
	data    []byte
	deleted bool
}
type writeBuff struct {
	buffered    map[ID]*buffItem
	accumulated uint64
	file        *os.File
}

func newWriteBuff(file *os.File) (c *writeBuff) {
	c = &writeBuff{}
	c.file = file
	c.reset()

	return
}

func (b *writeBuff) append(id ID, data []byte) {
	bi := buffItem{
		data: data,
	}
	b.buffered[id] = &bi
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
	b.accumulated = 0
}

func (b *writeBuff) size() uint64 {
	return b.accumulated
}

type idOffset struct {
	id     ID
	offset uint64
}

func (b *writeBuff) flush() (bo []idOffset, err error) {
	currentOffset := uint64(0)
	for id, bi := range b.buffered {
		if !bi.deleted {
			if _, err = b.file.Write(bi.data); err != nil {
				return
			}
			bo = append(bo, idOffset{id, currentOffset})
			currentOffset += uint64(len(bi.data))
		}
	}
	b.reset()
	return
}
