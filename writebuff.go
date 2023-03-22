package simpledb

import (
	"os"
)

// This is a sort-of bufio.Writer, but performing specific work on buffer flush
// it. the database must know what is still in the buffer and has not been saved to disk
type buffItem struct {
	data    []byte
	id      ID
	deleted bool
}
type writeBuff struct {
	buffData    []buffItem
	buffIndx    map[ID]*buffItem
	accumulated uint64
	file        *os.File
}

func newWriteBuff(file *os.File) (c *writeBuff) {
	c = &writeBuff{}
	c.file = file
	c.reset()

	return
}

func (b *writeBuff) reset() {
	b.buffData = make([]buffItem, 0)
	b.buffIndx = make(map[ID]*buffItem)
	b.accumulated = 0
}

func (b *writeBuff) append(id ID, data []byte) {
	bi := buffItem{data: data, id: id}
	b.buffData = append(b.buffData, bi)
	b.buffIndx[id] = &bi
	b.accumulated += uint64(len(data))
}

func (b *writeBuff) remove(id ID) {
	b.buffIndx[id].deleted = true
}
func (b *writeBuff) has(id ID) bool {
	_, buffered := b.buffIndx[id]
	if !buffered {
		return false
	}
	deleted := b.buffIndx[id].deleted
	return !deleted
}

func (b *writeBuff) size() uint64 {
	return b.accumulated
}

type idOffset struct {
	id     ID
	offset uint64
}

func (b *writeBuff) flush() ([]idOffset, error) {
	bo := make([]idOffset, 0)
	var err error
	currentOffset := uint64(0)
	if len(b.buffData) > 0 {
		for i := 0; i < len(b.buffData); i++ {
			if !b.buffData[i].deleted {
				if _, err = b.file.Write(b.buffData[i].data); err != nil {
					return nil, err
				}
				bo = append(bo, idOffset{b.buffData[i].id, currentOffset})
				// fmt.Println("bo", bo[len(bo)-1:])
				currentOffset += uint64(len(b.buffData[i].data))
			}
		}
		b.reset()
	}
	return bo, nil
}
