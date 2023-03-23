package simpledb

import "os"

// This is a sort-of bufio.Writer, but performing specific work on buffer flush
// it. the database must know what is still in the buffer and has not been saved to disk
type writeBuff struct {
	data        [][]byte
	blockIDs    []ID
	buffered    map[ID]struct{}
	deleted     map[ID]struct{}
	accumulated uint64
	file        *os.File
}

func newWriteBuff(file *os.File) (c *writeBuff) {
	c = &writeBuff{}
	c.file = file
	c.reset()

	return
}

func (b *writeBuff) grow(id ID, data []byte) {
	b.data = append(b.data, data)
	b.blockIDs = append(b.blockIDs, id)
	b.buffered[id] = struct{}{}
	b.accumulated += uint64(len(data))
}
func (b *writeBuff) remove(id ID) {
	b.deleted[id] = struct{}{}
}
func (b *writeBuff) has(id ID) bool {
	_, buffered := b.buffered[id]
	_, deleted := b.deleted[id]
	return buffered && !deleted
}

func (b *writeBuff) reset() {
	b.data = make([][]byte, 0)
	b.blockIDs = make([]ID, 0)
	b.buffered = make(map[ID]struct{})
	b.deleted = make(map[ID]struct{})
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
	if len(b.data) > 0 {
		for i := 0; i < len(b.data); i++ {
			if _, deleted := b.deleted[b.blockIDs[i]]; !deleted {
				el := b.data[i]
				if _, err = b.file.Write(el); err != nil {
					return
				}
				bo = append(bo, idOffset{b.blockIDs[i], currentOffset})
				currentOffset += uint64(len(el))
			}
		}
		b.reset()
	}
	return
}
