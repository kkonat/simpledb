package simpledb

type writeCache[T any] struct {
	cache[T]
	accumulated uint64
}

type itemToWrite[T any] struct {
	ID         ID
	Key        Key
	valueSrlzd []byte
}

func (i *itemToWrite[T]) itemSize() uint32 {
	if i.valueSrlzd != nil {
		return uint32(len(i.valueSrlzd) + len(i.Key) + blockheadersSize())
	} else {
		panic("not yet serialized")
	}
}

func newWriteCache[T any](CacheSize uint32) (c *writeCache[T]) {
	c = &writeCache[T]{}
	c.init(CacheSize)
	return
}

func (c *writeCache[T]) accumulate(item *itemToWrite[T]) {
	c.queue.PushBack(item)
	c.queueIndx[item.ID] = c.queue.Back()
	c.accumulated += uint64(item.itemSize())
}

func (c *writeCache[T]) reset() {
	c.queue.Init()
	for k := range c.queueIndx {
		delete(c.queueIndx, k)
	}
	c.accumulated = 0
}

func (c *writeCache[T]) size() uint64 {
	return c.accumulated
}
