package conshash

import (
	"fmt"
	"hash/crc64"
	"hash/fnv"
	"sort"

	"sync"
)

// ConsistentHashinger 一致性哈希(Consistent Hashing)算法
// 参考(算法介绍)[http://blog.csdn.net/cywosp/article/details/23397179/]
type ConsistentHashinger interface {
	// 添加一个元素
	Put(key string, value interface{}) ConsistentHashinger
	// 移除一个元素
	// 删除时的key必须核添加时的key保持一致
	Remove(key string) (value interface{})
	// 获取一个元素
	// key 任意一个key 不需要核添加时的key一致
	Get(key string) (outkey string, value interface{})
	// 获取所有的键值对
	Maps() map[string]interface{}
	// 获取所有的键
	Keys([]string) int
	// 获取所有的值
	Values([]interface{}) int
	// 元素数量
	Len() int
}

// 定义环类型
type _circle []uint64

func (c _circle) Len() int {
	return len(c)
}

func (c _circle) Less(i, j int) bool {
	return c[i] < c[j]
}

func (c _circle) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

var hasher = fnv.New64a()
var crchasher = crc64.New(crc64.MakeTable(0xfefefefe))

func defaultHash(data []byte) uint64 {
	hasher.Reset()
	_, _ = hasher.Write(data)
	return hasher.Sum64()
}

// CreateConsistentHashinger 创建一致性哈希(Consistent Hashing)算法生产器
func CreateConsistentHashinger(vnodenum int) ConsistentHashinger {

	return &consistentHashing{
		node:     make(map[string]interface{}),
		vnode:    make(map[uint64]string),
		nodev:    make(map[string][]uint64),
		vnodenum: vnodenum,
	}
}

type consistentHashing struct {
	node   map[string]interface{}
	vnode  map[uint64]string
	nodev  map[string][]uint64
	circle _circle
	sync.RWMutex
	//虚拟节点数量
	vnodenum int
}

// 添加一个元素
func (h *consistentHashing) Put(key string, value interface{}) ConsistentHashinger {
	h.RLock()
	_, ok := h.node[key]
	if ok {
		h.RUnlock()
		//已经存在
		return h
	}

	h.RUnlock()
	h.Lock()
	h.addCircle(key, value)
	h.Unlock()
	return h
}

// 移除一个元素
// 删除时的key必须核添加时的key保持一致
func (h *consistentHashing) Remove(key string) (value interface{}) {
	h.RLock()
	v, ok := h.node[key]
	if !ok {
		h.RUnlock()
		//不存在
		return nil
	}
	h.RUnlock()
	h.Lock()
	defer h.Unlock()
	h.removeCircle(key)
	return v
}

// 获取一个元素
// key 任意一个key 不需要核添加时的key一致
func (h *consistentHashing) Get(key string) (outkey string, value interface{}) {
	hashKey := defaultHash([]byte(key))
	h.RLock()
	defer h.RUnlock()

	i := h.search(hashKey)
	outkey = h.vnode[i]
	value = h.nodev[outkey]
	return
}
func (h *consistentHashing) search(key uint64) uint64 {
	f := func(x int) bool {
		return h.circle[x] >= key
	}

	i := sort.Search(len(h.circle), f)
	i = i - 1
	if i < 0 {
		i = len(h.circle) - 1
	}
	return h.circle[i]
}

// 获取所有的键值对
func (h *consistentHashing) Maps() map[string]interface{} {
	return h.node
}

// 获取所有的键
func (h *consistentHashing) Keys(keys []string) int {

	for k := range h.node {
		keys = append(keys, k)
	}

	return len(h.node)
}

// 获取所有的值
func (h *consistentHashing) Values(values []interface{}) int {
	for _, v := range h.node {
		values = append(values, v)
	}
	return len(h.node)
}

// 元素数量
func (h *consistentHashing) Len() int {
	return len(h.node)
}
func (h *consistentHashing) addCircle(key string, value interface{}) {
	h.node[key] = value
	vs, ok := h.nodev[key]
	if !ok {
		vs = make([]uint64, 0, h.vnodenum)
	}
	for i := 0; i < h.vnodenum; i++ {
		v := defaultHash([]byte(fmt.Sprintf("%s_%d", key, i)))
		h.vnode[v] = key
		vs = append(vs, v)
	}
	h.nodev[key] = vs
	h.updateCricle()
}
func (h *consistentHashing) updateCricle() {
	h.circle = _circle{}
	for k := range h.vnode {
		h.circle = append(h.circle, k)
	}
	sort.Sort(h.circle)
}

func (h *consistentHashing) removeCircle(key string) {
	vs, _ := h.nodev[key]
	for _, n := range vs {
		delete(h.vnode, n)
	}
	delete(h.nodev, key)
	delete(h.node, key)
	h.updateCricle()
}
