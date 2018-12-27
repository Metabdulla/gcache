package gcache

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

func newtestSimploOrderCache() *SimpleOrderedCache {
	c := New(50000).Simple().Expiration(time.Second * 10).BuildOrderedCache()
	return c.(*SimpleOrderedCache)

}

func TestSimpleOrderedCache_Remove(t *testing.T) {
	c := newtestSimploOrderCache()
	for i := 0; i < 1000; i++ {
		c.EnQueUe(i, fmt.Sprintf("%d", i))
	}
	t.Log(c.Len(), c.orderedKeys)
	index := []int{0}
	c.removeKeysByIndex(index)
	t.Log(c.Len(), c.orderedKeys)
	total := c.Len()
	for i := 0; i < total; i++ {
		c.DeQueUe()
	}
	t.Log(c.Len(), c.orderedKeys)
}

func TestSimpleOrderedCache_EnQueueBatch(t *testing.T) {
	c := newtestSimploOrderCache()
	for i := 0; i < 100; i++ {
		c.EnQueUe(i, fmt.Sprintf("%d", i))
	}
	var keys []interface{}
	var values []interface{}
	for i := 0; i < 100; i++ {
		keys = append(keys, 200+i)
		values = append(values, fmt.Sprintf("hi%d", i))
	}
	t.Log(len(keys), len(values))
	c.EnQueueBatch(keys, values)
	t.Log(c.Len(), c.orderedKeys)
	total := c.Len()
	for i := 0; i < total/2; i++ {
		c.DeQueUe()
	}
	t.Log(c.Len(), c.orderedKeys)
	keys, values, err := c.DeQueUeBatch(40)
	t.Log(len(keys), len(values), err)
	t.Log(len(c.orderedKeys), c.orderedKeys)
	var itemKeys []int
	for k := range c.items {
		itemKeys = append(itemKeys, k.(int))
	}
	sort.Ints(itemKeys)
	t.Log(len(itemKeys), itemKeys)
	t.Log(c.Len())
	val, err := c.GetIFPresent(256)
	t.Log(val, err)
}

func TestRemoveByIndex(t *testing.T) {
	c := newtestSimploOrderCache()
	var index []int
	for i := 0; i < 100; i++ {
		c.orderedKeys = append(c.orderedKeys, i)
		index = append(index, i)
	}
	c.removeKeysByIndex(index)
	if c.Len() != 0 {
		t.Fatalf("need 0, got %d %v, %v", c.Len(), c.orderedKeys)
	}
}
