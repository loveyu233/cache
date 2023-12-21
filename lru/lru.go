// Package lru 缓存淘汰策略
package lru

import "container/list"

type Value interface {
	Len() int
}

type Cache struct {
	maxBytes  int64 // 允许使用的最大内存
	useBytes  int64 // 当前已使用的内存
	ll        *list.List
	cache     map[string]*list.Element
	OnEvicted func(key string, value Value) // 某条记录被移除时的回调函数，可以为 nil
}

type entry struct {
	key   string
	value Value
}

func NewCache(maxBytes int64, onEvicted func(string, Value)) *Cache {
	return &Cache{
		maxBytes:  maxBytes,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
		OnEvicted: onEvicted,
	}
}

// Get 获取某个元素
func (c *Cache) Get(key string) (value Value, ok bool) {
	// 如果键对应的链表节点存在，则将对应节点移动到链表头部，并返回查找到的值
	if element, ok := c.cache[key]; ok {
		c.ll.MoveToFront(element)
		kv := element.Value.(*entry)
		return kv.value, true
	}
	return
}

// RemoveOldest 缓存淘汰。即移除最近最少访问的节点（队首）
func (c *Cache) RemoveOldest() {
	// 获取链表最后元素
	element := c.ll.Back()
	if element != nil {
		// 移除元素
		c.ll.Remove(element)
		kv := element.Value.(*entry)
		// 删除该元素的缓存
		delete(c.cache, kv.key)
		// 计算当前使用内存
		c.useBytes -= int64(len(kv.key)) + int64(kv.value.Len())
		// 回调函数不为空则调用
		if c.OnEvicted != nil {
			c.OnEvicted(kv.key, kv.value)
		}
	}
}

// Add 添加kv,如何k存在则更新v值
func (c *Cache) Add(key string, value Value) {
	// 判断cache中是否有改缓存
	if element, ok := c.cache[key]; ok {
		// 有则移动到链表头
		c.ll.MoveToFront(element)
		kv := element.Value.(*entry)
		// 重新计算当前内存使用
		c.useBytes += int64(value.Len()) - int64(kv.value.Len())
		// 更新值
		kv.value = value
	} else {
		// 没有缓存则添加新节点,添加位置为链表头
		element := c.ll.PushFront(&entry{key, value})
		// cache缓存改kv
		c.cache[key] = element
		// 计算当前内存使用
		c.useBytes += int64(len(key)) + int64(value.Len())
	}
	// 判断当前内存使用是否超出
	for c.maxBytes != 0 && c.maxBytes < c.useBytes {
		c.RemoveOldest()
	}
}

func (c *Cache) Len() int {
	return c.ll.Len()
}
