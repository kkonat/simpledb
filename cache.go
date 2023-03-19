package simpledb

import (
	"container/list"
	"fmt"

	"sync"

	"github.com/kkonat/simpledb/hash"
)

type Stats struct {
	requests uint64
	hits     uint64
}
type cache[T any] struct {
	data             map[ID]*Item[T]
	queue            *list.List
	queueIndx        map[ID]*list.Element
	statistics       Stats
	size             uint32
	addItem          func(item *Item[T])
	checkaAndGetItem func(id ID) (item *Item[T], ok bool)
	removeItem       func(id ID) (ok bool)
	mtx              sync.RWMutex
}

type Item[T any] struct {
	ID       ID
	LastUsed uint64
	KeyHash  hash.Type
	Key      Key
	Value    *T
}

func newCache[T any](CacheSize uint32) (c *cache[T]) {
	c = &cache[T]{}
	if CacheSize == 0 { // if no cache is to be created, set dummy functions, to eliminate frequent `if cache == nil`` checks
		c.addItem = func(item *Item[T]) {}
		c.checkaAndGetItem = func(id ID) (item *Item[T], ok bool) { return nil, false }
		c.removeItem = func(id ID) (ok bool) { return true }
	} else {
		c.addItem = c.add
		c.checkaAndGetItem = c.checkAndGet
		c.removeItem = c.remove
		// only create the map and slice, if cache is actually created
		c.size = CacheSize
		c.data = make(map[ID]*Item[T])
		c.queueIndx = make(map[ID]*list.Element)
		c.queue = list.New()
	}
	return
}

// cleans up the cache
func (c *cache[T]) cleanup() {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	// mark unused for GC
	for i := range c.data {
		c.data[i] = nil
	}
	c.data = nil
	c.queue.Init()
	c.queue = nil
	for i := range c.queueIndx {
		c.queueIndx[i] = nil
	}
}

// adds new item to the cache and drops the oldest one
func (c *cache[T]) add(item *Item[T]) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if uint32(c.queue.Len()) == c.size {
		first := c.queue.Front()
		firstId, _ := first.Value.(ID)
		delete(c.data, firstId)
		c.queue.Remove(first)
		delete(c.queueIndx, firstId)
	}
	c.data[item.ID] = item
	c.queue.PushBack(item.ID)
	c.queueIndx[item.ID] = c.queue.Back()
}

// checks if the item is in the cache and if so, returns its value
func (c *cache[T]) checkAndGet(id ID) (item *Item[T], ok bool) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	c.statistics.requests++
	if item, ok = c.data[id]; ok {
		c.statistics.hits++
	}
	return
}

// touches an element in the queue and marks it as the one used most recently (i.e. puts it at the end of the queue)
func (c *cache[T]) touch(id ID) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.queue.Len() < 2 {
		return
	}
	// find that ID in the queue
	el := c.queueIndx[id]
	c.queue.Remove(el)
	c.queue.PushBack(el.Value)
	c.queueIndx[id] = c.queue.Back()
}

// removes an item with given id from cache
func (c *cache[T]) remove(id ID) (ok bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	delete(c.data, id)        // dlelete data item
	el, ok := c.queueIndx[id] // find el in queue using index)
	if ok {
		c.queue.Remove(el)      // delete el in queue
		delete(c.queueIndx, id) // delete el in index
	} else {
		panic(fmt.Sprintf("no el %d in queue", id))
	}
	return
}

// Gets rudimentary cache stats
func (c *cache[T]) GetHitRate() float64 {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	if c.statistics.requests > 0 {
		return float64(c.statistics.hits) / float64(c.statistics.requests) * 100
	} else {
		return 0
	}
}
