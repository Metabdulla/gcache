package gcache

import (
	"fmt"
	"time"
)

// SimpleOrderedCache has no clear priority for evict cache. It depends on key-value map order.
type SimpleOrderedCache struct {
	baseCache
	items       map[interface{}]*simpleItem
	orderedKeys []interface{}
}

func newSimpleOrderedCache(cb *CacheBuilder) *SimpleOrderedCache {
	c := &SimpleOrderedCache{}
	buildCache(&c.baseCache, cb)

	c.init()
	c.loadGroup.orderedCache = c
	return c
}

func (c *SimpleOrderedCache) init() {
	if c.size <= 0 {
		c.items = make(map[interface{}]*simpleItem)
	} else {
		c.items = make(map[interface{}]*simpleItem, c.size)
	}
	c.orderedKeys = nil
}

func (c *SimpleOrderedCache) addFront(key, value interface{}) (interface{}, error) {
	var err error
	if c.serializeFunc != nil {
		value, err = c.serializeFunc(key, value)
		if err != nil {
			return nil, err
		}
	}

	// Check for existing item
	item, ok := c.items[key]
	if ok {
		item.value = value
	} else {
		// Verify size not exceeded
		if (len(c.items) >= c.size) && c.size > 0 {
			c.evict(1)
			if len(c.items) >= c.size {
				return nil, ReachedMaxSizeErr
			}
		}
		item = &simpleItem{
			clock: c.clock,
			value: value,
		}
		c.items[key] = item
		c.orderedKeys = append([]interface{}{key}, c.orderedKeys...)
	}

	if c.expiration != nil {
		t := c.clock.Now().Add(*c.expiration)
		item.expiration = &t
	}

	if c.addedFunc != nil {
		c.addedFunc(key, value)
	}

	return item, nil
}

func (c *SimpleOrderedCache) enQueue(key, value interface{}) (interface{}, error) {
	var err error
	if c.serializeFunc != nil {
		value, err = c.serializeFunc(key, value)
		if err != nil {
			return nil, err
		}
	}

	// Check for existing item
	item, ok := c.items[key]
	if ok {
		item.value = value
	} else {
		// Verify size not exceeded
		if (len(c.items) >= c.size) && c.size > 0 {
			c.evict(1)
			if len(c.items) >= c.size {
				return nil, ReachedMaxSizeErr
			}
		}
		item = &simpleItem{
			clock: c.clock,
			value: value,
		}
		c.items[key] = item
		c.orderedKeys = append(c.orderedKeys, key)
	}

	if c.expiration != nil {
		t := c.clock.Now().Add(*c.expiration)
		item.expiration = &t
	}

	if c.addedFunc != nil {
		c.addedFunc(key, value)
	}

	return item, nil
}

func (c *SimpleOrderedCache) enQueueBatch(keys []interface{}, values []interface{}) error {
	if len(values) != len(keys) {
		panic("len(keys) != len(values)")
	}
	evicted := false
	for i, key := range keys {
		value := values[i]
		var err error
		if c.serializeFunc != nil {
			value, err = c.serializeFunc(key, value)
			if err != nil {
				return err
			}
		}

		// Check for existing item
		item, ok := c.items[key]
		if ok {
			item.value = value
		} else {
			// Verify size not exceeded
			if (len(c.items) >= c.size) && c.size > 0 {
				if !evicted {
					c.evict(len(keys) - i)
					evicted = true
				}
				if len(c.items) >= c.size {
					return ReachedMaxSizeErr
				}
			}
			item = &simpleItem{
				clock: c.clock,
				value: value,
			}
			c.items[key] = item
			c.orderedKeys = append(c.orderedKeys, key)
		}

		if c.expiration != nil {
			t := c.clock.Now().Add(*c.expiration)
			item.expiration = &t
		}

		if c.addedFunc != nil {
			c.addedFunc(key, value)
		}
	}
	return nil
}

func (c *SimpleOrderedCache) addFrontBatch(keys []interface{}, values []interface{}) error {
	if len(values) != len(keys) {
		panic("len(keys) != len(values)")
	}
	evicted := false
	var insertKeys  []interface{}
	for i, key := range keys {
		value := values[i]
		var err error
		if c.serializeFunc != nil {
			value, err = c.serializeFunc(key, value)
			if err != nil {
				return err
			}
		}

		// Check for existing item
		item, ok := c.items[key]
		if ok {
			item.value = value
		} else {
			// Verify size not exceeded
			if (len(c.items) >= c.size) && c.size > 0 {
				if !evicted {
					c.evict(len(keys) - i)
					evicted = true
				}
				if len(c.items) >= c.size {
					return ReachedMaxSizeErr
				}
			}
			item = &simpleItem{
				clock: c.clock,
				value: value,
			}
			c.items[key] = item
			insertKeys= append(insertKeys,key)
		}

		if c.expiration != nil {
			t := c.clock.Now().Add(*c.expiration)
			item.expiration = &t
		}

		if c.addedFunc != nil {
			c.addedFunc(key, value)
		}
	}
	c.orderedKeys = append(insertKeys, c.orderedKeys...,)
	return nil
}

// Get a value from cache pool using key if it exists.
// If it dose not exists key and has LoaderFunc,
// generate a value using `LoaderFunc` method returns value.
func (c *SimpleOrderedCache) Get(key interface{}) (interface{}, error) {
	v, err := c.get(key, false)
	if err == KeyNotFoundError {
		return c.getWithLoader(key, true)
	}
	return v, err
}

// Get a value from cache pool using key if it exists.
// If it dose not exists key, returns KeyNotFoundError.
// And send a request which refresh value for specified key if cache object has LoaderFunc.
func (c *SimpleOrderedCache) GetIFPresent(key interface{}) (interface{}, error) {
	v, err := c.get(key, false)
	if err == KeyNotFoundError {
		return c.getWithLoader(key, false)
	}
	return v, nil
}

func (c *SimpleOrderedCache) get(key interface{}, onLoad bool) (interface{}, error) {
	v, err := c.getValue(key, onLoad)
	if err != nil {
		return nil, err
	}
	if c.deserializeFunc != nil {
		return c.deserializeFunc(key, v)
	}
	return v, nil
}

func (c *SimpleOrderedCache) getValue(key interface{}, onLoad bool) (interface{}, error) {
	c.mu.Lock()
	item, ok := c.items[key]
	if ok {
		if !item.IsExpired(nil) {
			v := item.value
			c.mu.Unlock()
			if !onLoad {
				c.stats.IncrHitCount()
			}
			return v, nil
		}
		//todo remove ordered key
		c.remove(key)
	}
	c.mu.Unlock()
	if !onLoad {
		c.stats.IncrMissCount()
	}
	return nil, KeyNotFoundError
}

func (c *SimpleOrderedCache) getWithLoader(key interface{}, isWait bool) (interface{}, error) {
	if c.loaderExpireFunc == nil {
		return nil, KeyNotFoundError
	}
	value, _, err := c.load(key, func(v interface{}, expiration *time.Duration, e error) (interface{}, error) {
		if e != nil {
			return nil, e
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		item, err := c.enQueue(key, v)
		if err != nil {
			return nil, err
		}
		if expiration != nil {
			t := c.clock.Now().Add(*expiration)
			item.(*simpleItem).expiration = &t
		}
		return v, nil
	}, isWait)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (c *SimpleOrderedCache) evict(count int) {
	now := c.clock.Now()
	current := 0
	var fail int
	var removedKeys []int
	for i := 0; i < len(c.orderedKeys); i++ {
		if current >= count || fail > 3*count {
			c.removeKeysByIndex(removedKeys)
			return
		}
		key := c.orderedKeys[i]
		item, ok := c.items[key]
		if ok {
			if item.expiration == nil || now.After(*item.expiration) ||
				(c.expireFunction != nil && c.expireFunction(key)) {
				defer c.remove(key)
				removedKeys = append(removedKeys, i)
				current++
			} else {
				fail++
			}
		} else {
			removedKeys = append(removedKeys, i)
			current++
		}
	}

}

//removeKeysByIndex
//[][][][][remove][remove][]
func (c *SimpleOrderedCache) removeKeysByIndex(Index []int) {
	if len(Index) == 0 {
		return
	}
	if len(Index) > len(c.orderedKeys) {
		panic("all elements will be removed")
	}
	oldKeys := c.orderedKeys
	start := Index[0]
	end := Index[len(Index)-1]
	c.orderedKeys = oldKeys[:start]
	first := start
	for i, second := range Index {
		//just process [][][][][remove][][remove][]
		if i == 0 {
			continue
		}
		if first >= second {
			panic("must be sorted")
		}
		//don't append in this case  [][][][][j][k][]
		if first+1 < second {
			c.orderedKeys = append(c.orderedKeys, oldKeys[first+1:second]...)
		}
		first = second
	}
	c.orderedKeys = append(c.orderedKeys, oldKeys[end+1:]...)
}

// Removes the provided key from the cache.
func (c *SimpleOrderedCache) Remove(key interface{}) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ok := c.remove(key)
	if len(c.items) == 0 {
		c.orderedKeys = nil
	} else if len(c.items) == 1 {
		c.orderedKeys = nil
		for k := range c.items {
			c.orderedKeys = append(c.orderedKeys, k)
		}
	}
	return ok
}

func (c *SimpleOrderedCache) remove(key interface{}) bool {
	item, ok := c.items[key]
	if ok {
		delete(c.items, key)
		if c.evictedFunc != nil {
			c.evictedFunc(key, item.value)
		}
		return true
	}
	return false
}

// Returns a slice of the keys in the cache.
func (c *SimpleOrderedCache) keys() []interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]interface{}, len(c.items))
	var i = 0
	for k := range c.items {
		keys[i] = k
		i++
	}
	return keys
}

// Returns a slice of the keys in the cache.
func (c *SimpleOrderedCache) Keys() []interface{} {
	keys := []interface{}{}
	for _, k := range c.keys() {
		_, err := c.GetIFPresent(k)
		if err == nil {
			keys = append(keys, k)
		}
	}
	return keys
}

// Returns all key-value pairs in the cache.
func (c *SimpleOrderedCache) GetALL() map[interface{}]interface{} {
	m := make(map[interface{}]interface{})
	for _, k := range c.keys() {
		v, err := c.GetIFPresent(k)
		if err == nil {
			m[k] = v
		}
	}
	return m
}

func (c *SimpleOrderedCache) GetKeysAndValues() (keys []interface{}, values []interface{}) {
	return c.getALl()
}

func (c *SimpleOrderedCache) Refresh() {
	c.getALl()
}

func (c *SimpleOrderedCache) len() (keyLen, itemLen int) {
	c.mu.Lock()
	keyLen = len(c.orderedKeys)
	itemLen = len(c.items)
	c.mu.Unlock()
	if itemLen > keyLen {
		panic(fmt.Sprintf("item len must be smaller itemlen %d, keyLen %d", itemLen, keyLen))
	}
	return keyLen, itemLen
}

// Returns the number of items in the cache.
func (c *SimpleOrderedCache) Len() int {
	//return len(c.GetALL())
	keyLen, itemLen := c.len()
	if itemLen < keyLen/2 {
		c.getALl()
	}
	keyLen, itemLen = c.len()
	return keyLen
}

// Completely clear the cache
func (c *SimpleOrderedCache) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.purgeVisitorFunc != nil {
		for key, item := range c.items {
			c.purgeVisitorFunc(key, item.value)
		}
	}

	c.init()
}

func (c *SimpleOrderedCache) OrderedKeys() []interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.orderedKeys
}

func (c *SimpleOrderedCache) moveFront(key interface{}) (err error) {
	var try = 2
	c.mu.Lock()
	_, ok := c.items[key]
	if !ok {
		defer c.stats.IncrMissCount()
	}
	defer c.mu.Unlock()
	if ok {
		if len(c.orderedKeys) ==0 {
			return EmptyErr
		}
		//if key is first two item ,don't move it
		for i:=0;i<try && i< len( c.orderedKeys);i++ {
			head :=c.orderedKeys[i]
			if head == key {
				return nil
			}
		}
		if len(c.orderedKeys) >= c.size {
			err = ReachedMaxSizeErr
		}
		arr:= []interface{}{key}
		c.orderedKeys = append(arr,c.orderedKeys...)
		return err
	}
	return KeyNotFoundError
}

func (c *SimpleOrderedCache) MoveFront(key interface{}) error {
	return c.moveFront(key)
}

func (c *SimpleOrderedCache) getALl() (keys []interface{}, values []interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var index []int
	for i, key := range c.orderedKeys {
		item, ok := c.items[key]
		if ok {
			if !item.IsExpired(nil) {
				v := item.value
				if c.deserializeFunc != nil {
					v, _ = c.deserializeFunc(key, v)
				}
				values = append(values, v)
				keys = append(keys, key)
				//c.stats.IncrHitCount()
			} else {
				//todo remove ordered key
				index = append(index, i)
				c.remove(key)
			}
		} else {
			//c.stats.IncrMissCount()
			index = append(index, i)
		}
	}
	c.removeKeysByIndex(index)
	return keys, values
}

func (c *SimpleOrderedCache) getTop() (key interface{}, value interface{}, err error) {
	c.mu.Lock()
	var removedIndex []int
	for i, k := range c.orderedKeys {
		//key =  c.keyTypeFunction(k)
		key = k
		item, ok := c.items[key]
		if ok {
			value = item.value
			if item.IsExpired(nil) {
				removedIndex = append(removedIndex, i)
				c.remove(key)
			}
			break
		}
		removedIndex = append(removedIndex, i)
		c.remove(key)
	}
	c.removeKeysByIndex(removedIndex)
	c.mu.Unlock()
	if value != nil {
		c.stats.IncrHitCount()
		return key, value, nil
	} else {
		c.stats.IncrMissCount()
		return nil, nil, EmptyErr
	}
}

func (c *SimpleOrderedCache) GetTop() (key interface{}, value interface{}, err error) {
	return c.getTop()
}

func (c *SimpleOrderedCache) deQueueBatch(count int) (keys []interface{}, values []interface{}, err error) {
	c.mu.Lock()
	var removedIndex []int
	current := 0
	all := false
	if count >= len(c.orderedKeys) {
		all = true
	}
	for i, key := range c.orderedKeys {
		item, ok := c.items[key]
		if ok {
			if current >= count {
				break
			}
			value := item.value
			values = append(values, value)
			keys = append(keys, key)
			current++
		}
		removedIndex = append(removedIndex, i)
		c.remove(key)
	}
	fmt.Println(removedIndex)
	if all {
		c.init()
	} else {
		c.removeKeysByIndex(removedIndex)
	}
	c.mu.Unlock()
	if len(values) != 0 {
		c.stats.IncrHitCount()
		return keys, values, nil
	} else {
		c.stats.IncrMissCount()
		return nil, nil, EmptyErr
	}
}

func (c *SimpleOrderedCache) deQueue() (key interface{}, value interface{}, err error) {
	c.mu.Lock()
	var removedIndex []int
	for i, key := range c.orderedKeys {
		removedIndex = append(removedIndex, i)
		item, ok := c.items[key]
		if ok {
			value = item.value
			c.remove(key)
			break
		}
		c.remove(key)
	}
	c.removeKeysByIndex(removedIndex)
	c.mu.Unlock()
	if value != nil {
		c.stats.IncrHitCount()
		return key, value, nil
	} else {
		c.stats.IncrMissCount()
		return key, nil, EmptyErr
	}
}

func (c *SimpleOrderedCache) DeQueue() (key interface{}, value interface{}, err error) {
	return c.deQueue()
}

func (c *SimpleOrderedCache) DeQueueBatch(count int) (keys []interface{}, values []interface{}, err error) {
	return c.deQueueBatch(count)
}

func (c *SimpleOrderedCache) EnQueue(key interface{}, value interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.enQueue(key, value)
	return err
}

func (c *SimpleOrderedCache) EnQueueBatch(keys []interface{}, values []interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.enQueueBatch(keys, values)
}

func (c *SimpleOrderedCache) AddFrontBatch(keys []interface{}, values []interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.addFrontBatch(keys, values)
}


func ( c*SimpleOrderedCache)AddFront(key interface{}, value interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.addFront(key, value)
	return err
}

func (c *SimpleOrderedCache) RemoveExpired(allowFailCount int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.removeExpired(allowFailCount)
}

func (c *SimpleOrderedCache) removeExpired(allowFailCount int) error {
	var try int
	var removedIndex []int
	for i, k := range c.orderedKeys {
		//key :=  c.keyTypeFunction(k)
		key := k
		if try > allowFailCount {
			break
		}
		item, ok := c.items[key]
		if ok {
			if item.IsExpired(nil) {
				removedIndex = append(removedIndex, i)
				c.remove(key)
				continue
			}
			if c.expireFunction != nil {
				if c.expireFunction(key) {
					removedIndex = append(removedIndex, i)
					c.remove(key)
					continue
				}
			}
			try++
		}
		c.remove(key)
	}
	c.removeKeysByIndex(removedIndex)
	return nil
}
