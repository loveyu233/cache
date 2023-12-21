// Package consistenthash 实现一致性哈希
package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type Hash func(data []byte) uint32

type Map struct {
	hash     Hash           // hash函数
	replicas int            // 副本数
	keys     []int          // hash环
	hashMap  map[int]string // 虚拟节点与真实节点的映射表,键是虚拟节点的哈希值，值是真实节点的名称
}

func NewMap(replicas int, fn ...Hash) *Map {
	m := &Map{replicas: replicas, hashMap: make(map[int]string)}
	if len(fn) == 0 || fn == nil {
		m.hash = crc32.ChecksumIEEE
	} else {
		m.hash = fn[0]
	}
	return m
}

// Add 添加真实节点
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		// 对每一个真实节点 key，对应创建 m.replicas 个虚拟节点，虚拟节点的名称是：strconv.Itoa(i) + key，即通过添加编号的方式区分不同虚拟节点。
		for i := 0; i < m.replicas; i++ {
			// 使用 m.hash() 计算虚拟节点的哈希值
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			// 使用 append(m.keys, hash) 添加到环上
			m.keys = append(m.keys, hash)
			// 添加映射表
			m.hashMap[hash] = key
		}
	}
	// 环上的哈希值排序
	sort.Ints(m.keys)
}

func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return ""
	}
	// 计算 key 的哈希值
	hash := int(m.hash([]byte(key)))
	// 顺时针找到第一个匹配的虚拟节点的下标 idx
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	// 如果 idx == len(m.keys)，说明应选择 m.keys[0]，因为 m.keys 是一个环状结构，所以用取余数的方式来处理这种情况
	return m.hashMap[m.keys[idx%len(m.keys)]]
}
