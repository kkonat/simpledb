package simpledb

type cacheStats struct {
	requests uint64
	hits     uint64
}
type cache[T any] struct {
	data             map[ID]*cacheItem[T]
	queue            []ID
	statistics       cacheStats
	size             uint32
	addItem          func(item *cacheItem[T])
	checkaAndGetItem func(id ID) (item *cacheItem[T], ok bool)
	removeItem       func(id ID) (ok bool)
}

type cacheItem[T any] struct {
	ID       ID
	LastUsed uint64
	KeyHash  Hash
	Key      Key
	Value    *T
}

func newCache[T any](CacheSize uint32) (c *cache[T]) {
	c = &cache[T]{}

	if CacheSize == 0 { // if no cache, set dummy functions, to eluminate frequent  constant if cache == null checking
		c.addItem = func(item *cacheItem[T]) {}
		c.checkaAndGetItem = func(id ID) (item *cacheItem[T], ok bool) { return nil, false }
		c.removeItem = func(id ID) (ok bool) { return true }
	} else {
		c.addItem = c.addItemCache
		c.checkaAndGetItem = c.checkaAndGetItemCache
		c.removeItem = c.removeItemCache
		c.size = CacheSize
		c.data = make(map[ID]*cacheItem[T])
		c.queue = make([]ID, 0)
	}
	return
}

func (c *cache[T]) cleanup() {

	// mark unused for GC
	for i := range c.data {
		c.data[i] = nil
	}
	c.data = nil
	c.queue = nil
}

func (c *cache[T]) addItemCache(item *cacheItem[T]) {
	if uint32(len(c.queue)) == c.size {
		delete(c.data, c.queue[0])
		c.queue = c.queue[1:]
	}
	c.data[item.ID] = item
	c.queue = append(c.queue, item.ID)
}

func (c *cache[T]) checkaAndGetItemCache(id ID) (item *cacheItem[T], ok bool) {
	c.statistics.requests++
	if item, ok = c.data[id]; ok {
		c.statistics.hits++
	}
	return
}

func (c *cache[T]) removeItemCache(id ID) (ok bool) {
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
	if m.statistics.requests > 0 {
		return float64(m.statistics.hits) / float64(m.statistics.requests) * 100
	} else {
		return 0
	}
}
