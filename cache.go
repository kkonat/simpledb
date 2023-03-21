package simpledb

import (
	"container/list"
	"fmt"

	"github.com/kkonat/simpledb/hash"
)

type Item[T any] struct {
	ID      ID
	KeyHash hash.Type
	Key     Key
	Value   *T
}

type Stats struct {
	requests uint64
	hits     uint64
}
type cache[T any] struct {
	queue      *list.List
	queueIndx  map[ID]*list.Element
	statistics Stats
	maxSize    uint32
}

func newCache[T any](CacheSize uint32) (c *cache[T]) {
	c = &cache[T]{}
	c.init(CacheSize)
	return
}

func (c *cache[T]) init(CacheSize uint32) {
	// only create the map and slice, if cache is actually created
	c.maxSize = CacheSize
	c.queueIndx = make(map[ID]*list.Element)
	c.queue = list.New()
	c.statistics = Stats{}
}

// cleans up the cache
func (c *cache[T]) cleanup() {
	// mark unused for GC
	for _, i := range c.queueIndx {
		i.Value = nil
	}
	c.queue.Init()
}

// adds new item to the cache and drops the oldest one
func (c *cache[T]) add(item *Item[T]) {

	if uint32(c.queue.Len()) == c.maxSize {
		first := c.queue.Front()
		firstId := first.Value.(*Item[T]).ID
		delete(c.queueIndx, firstId) // delete reference first
		c.queue.Remove(first)        // delete actual item
	}
	c.queue.PushBack(item)
	c.queueIndx[item.ID] = c.queue.Back()
}

// checks if the item is in the cache and if so, returns its value
func (c *cache[T]) checkAndGet(id ID) (*Item[T], bool) {
	c.statistics.requests++
	if item, ok := c.queueIndx[id]; ok {
		c.statistics.hits++
		return item.Value.(*Item[T]), true
	}
	return nil, false
}
func (c *cache[T]) check(id ID) (exists bool) {
	_, exists = c.queueIndx[id]
	return
}

// moves an element in the queue to its end to mars it as the one used most recently
func (c *cache[T]) touch(id ID) {

	if c.queue.Len() <= 1 {
		return
	}
	el := c.queueIndx[id]
	delete(c.queueIndx, id)
	c.queue.Remove(el)
	c.queue.PushBack(el.Value)
	c.queueIndx[id] = c.queue.Back()
}

// removes an item with given id from cache
func (c *cache[T]) remove(id ID) (ok bool) {

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
	if c.statistics.requests > 0 {
		return float64(c.statistics.hits) / float64(c.statistics.requests) * 100
	} else {
		return 0
	}
}
