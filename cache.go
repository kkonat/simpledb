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

	if CacheSize == 0 { // if no cache is to be created, set dummy functions, to eliminate frequent `if cache == nil`` checks
		c.addItem = func(item *cacheItem[T]) {}
		c.checkaAndGetItem = func(id ID) (item *cacheItem[T], ok bool) { return nil, false }
		c.removeItem = func(id ID) (ok bool) { return true }
	} else {
		c.addItem = c.addItemCache
		c.checkaAndGetItem = c.checkaAndGetItemCache
		c.removeItem = c.removeItemCache
		// only create the map and slice, if cache is actually created
		c.size = CacheSize
		c.data = make(map[ID]*cacheItem[T])
		c.queue = make([]ID, 0)
	}
	return
}

// cleans up the cache
func (c *cache[T]) cleanup() {

	// mark unused for GC
	for i := range c.data {
		c.data[i] = nil
	}
	c.data = nil
	c.queue = nil
}

// adds new item to the cache and drops the oldest one
func (c *cache[T]) addItemCache(item *cacheItem[T]) {
	if uint32(len(c.queue)) == c.size {
		delete(c.data, c.queue[0])
		c.queue = c.queue[1:]
	}
	c.data[item.ID] = item
	c.queue = append(c.queue, item.ID)
}

// checks if the item is in the cache and if so, returns its value
func (c *cache[T]) checkaAndGetItemCache(id ID) (item *cacheItem[T], ok bool) {
	c.statistics.requests++
	if item, ok = c.data[id]; ok {
		c.statistics.hits++
	}
	return
}

// touches an element in the queue and marks it as the one used most recently (i.e. puts it at the end of the queue)
func (c *cache[T]) touch(id ID) {
	if len(c.queue) < 2 {
		return
	}
	// find that ID in the queue
	for at, found := range c.queue {
		if found == id {
			c.queue = append(c.queue[:at], c.queue[at+1:]...)
			c.queue = append(c.queue, id)
			return
		}
	}
}

// removes an item with given id from cache
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

// Gets rudimentary cache stats
func (m cache[T]) GetHitRate() float64 {
	if m.statistics.requests > 0 {
		return float64(m.statistics.hits) / float64(m.statistics.requests) * 100
	} else {
		return 0
	}
}
