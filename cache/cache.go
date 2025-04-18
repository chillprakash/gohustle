package cache

import (
	"sync"
	"time"
)

type InMemoryCache struct {
	sync.RWMutex
	items map[string]Item
}

type Item struct {
	Value      interface{}
	Expiration int64
}

var (
	instance *InMemoryCache
	once     sync.Once
)

// GetInMemoryCacheInstance returns the singleton instance of InMemoryCache
func GetInMemoryCacheInstance() *InMemoryCache {
	once.Do(func() {
		instance = &InMemoryCache{
			items: make(map[string]Item),
		}
	})
	return instance
}

func (c *InMemoryCache) Set(key string, value interface{}, duration time.Duration) {
	c.Lock()
	defer c.Unlock()

	expiration := time.Now().Add(duration).UnixNano()
	c.items[key] = Item{
		Value:      value,
		Expiration: expiration,
	}
}

func (c *InMemoryCache) Get(key string) (interface{}, bool) {
	c.RLock()
	defer c.RUnlock()

	item, exists := c.items[key]
	if !exists {
		return nil, false
	}

	if time.Now().UnixNano() > item.Expiration {
		return nil, false
	}

	return item.Value, true
}

func (c *InMemoryCache) Delete(key string) {
	c.Lock()
	defer c.Unlock()
	delete(c.items, key)
}
