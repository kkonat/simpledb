package simpledb

const CacheMaxItems = 100

type cache[T any] struct {
	data     map[ID]*cacheItem[T]
	queue    []ID
	requests uint64
	hits     uint64
}

type cacheItem[T any] struct {
	ID       ID
	LastUsed uint64
	KeyHash  uint32
	Key      []byte
	Value    *T
}

func (c *cache[T]) initialize() {
	if c.data == nil {
		c.data = make(map[ID]*cacheItem[T])
	} else {
		panic("reinitializing cache")
	}
	if c.queue == nil {
		c.queue = make([]ID, 0)
	} else {
		panic("reinitializing cache")
	}
}
func (c *cache[T]) cleanup() {

	// mark unused for GC
	for i := range c.data {
		c.data[i] = nil
	}
	c.data = nil
	c.queue = nil
}

func (c *cache[T]) addItem(item *cacheItem[T]) {
	if len(c.queue) == CacheMaxItems {
		delete(c.data, c.queue[0])
		c.queue = c.queue[1:]
	}
	c.data[item.ID] = item
	c.queue = append(c.queue, item.ID)
}

func (c *cache[T]) getItem(id ID) (item *cacheItem[T], ok bool) {
	c.requests++
	if item, ok = c.data[id]; ok {
		c.hits++
	}
	return
}

func (c *cache[T]) removeItem(id ID) (ok bool) {
	delete(c.data, id)

	//remove id from queue
	for i := 0; i < len(c.queue); i++ {
		if c.queue[i] == id { // delete from queue
			c.queue = append(c.queue[:i], c.queue[i+1:]...)
			ok = true
			break
		}
	}
	return
}

func (m cache[T]) GetHitRate() float64 {
	if m.requests > 0 {
		return float64(m.hits) / float64(m.requests) * 100
	} else {
		return 0
	}
}
