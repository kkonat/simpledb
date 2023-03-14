package simpledb

type cacheStats struct {
	requests uint64
	hits     uint64
}
type cache[T any] struct {
	data            map[ID]*cacheItem[T]
	queue           []ID
	statistics      cacheStats
	size            uint32
	addFunc         func(item *cacheItem[T])
	checkAndGetFunc func(id ID) (item *cacheItem[T], ok bool)
	removeItemFunc  func(id ID) (ok bool)
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
		c.addFunc = c.addItemNoCache
		c.checkAndGetFunc = c.checkaAndGetItemNoCache
		c.removeItemFunc = c.removeItemNoCache
	} else {
		c.addFunc = c.addItemCache
		c.checkAndGetFunc = c.checkaAndGetItemCache
		c.removeItemFunc = c.removeItemCache
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

func (c *cache[T]) addItem(item *cacheItem[T]) {
	c.addFunc(item)
}
func (c *cache[T]) addItemCache(item *cacheItem[T]) {
	if uint32(len(c.queue)) == c.size {
		delete(c.data, c.queue[0])
		c.queue = c.queue[1:]
	}
	c.data[item.ID] = item
	c.queue = append(c.queue, item.ID)
}
func (c *cache[T]) addItemNoCache(item *cacheItem[T]) {
}

func (c *cache[T]) checkaAndGetItem(id ID) (item *cacheItem[T], ok bool) {
	return c.checkAndGetFunc(id)
}
func (c *cache[T]) checkaAndGetItemCache(id ID) (item *cacheItem[T], ok bool) {
	c.statistics.requests++
	if item, ok = c.data[id]; ok {
		c.statistics.hits++
	}
	return
}
func (c *cache[T]) checkaAndGetItemNoCache(id ID) (item *cacheItem[T], ok bool) {
	c.statistics.requests++
	return nil, false
}

func (c *cache[T]) removeItem(id ID) (ok bool) {
	return c.removeItemFunc(id)
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
func (c *cache[T]) removeItemNoCache(id ID) (ok bool) {
	return true // make no fuss
}

func (m cache[T]) GetHitRate() float64 {
	if m.statistics.requests > 0 {
		return float64(m.statistics.hits) / float64(m.statistics.requests) * 100
	} else {
		return 0
	}
}
