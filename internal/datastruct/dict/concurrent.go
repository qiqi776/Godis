package dict

import (
	"godis/pkg/wildcard"
	"math"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
)

const prime32 = uint32(16777619)

type shard struct {
	m  map[string]interface{}
	mx sync.RWMutex
}

type ConcurrentDict struct {
	table      []*shard
	count      int32
	shardCount int
}

// MakeConcurrent 创建一个分片并发字典
func MakeConcurrent(shardCount int) *ConcurrentDict {
	if shardCount == 1 {
		table := []*shard{
			{m: make(map[string]interface{})},
		}
		return &ConcurrentDict{
			count:      0,
			table:      table,
			shardCount: shardCount,
		}
	}
	shardCount = computeCapacity(shardCount)
	table := make([]*shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &shard{
			m: make(map[string]interface{}),
		}
	}
	return &ConcurrentDict{
		count:      0,
		table:      table,
		shardCount: shardCount,
	}
}

// computeCapacity 将分片数量向上取整为 2 的幂
func computeCapacity(param int) (size int) {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	}
	return n + 1
}

// fnv32 计算字符串的 FNV-1a 32 位哈希值
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// spread 根据 key 计算其所属的分片索引
func (dict *ConcurrentDict) spread(key string) uint32 {
	if dict == nil {
		panic("dict is nil")
	}
	if len(dict.table) == 1 {
		return 0
	}
	hashCode := fnv32(key)
	tableSize := uint32(len(dict.table))
	return (tableSize - 1) & hashCode
}

// getShard 根据索引获取分片
func (dict *ConcurrentDict) getShard(index uint32) *shard {
	if dict == nil {
		panic("dict is nil")
	}
	return dict.table[index]
}

// Get 线程安全地获取指定 key 的值
func (dict *ConcurrentDict) Get(key string) (val interface{}, exists bool) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	s.mx.RLock()
	defer s.mx.RUnlock()
	val, exists = s.m[key]
	return
}

// GetWithLock 在外部已加锁的情况下获取 key
func (dict *ConcurrentDict) GetWithLock(key string) (val interface{}, exists bool) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	val, exists = s.m[key]
	return
}

// Len 返回字典中 key 的数量
func (dict *ConcurrentDict) Len() int {
	if dict == nil {
		panic("dict is nil")
	}
	return int(atomic.LoadInt32(&dict.count))
}

// addCount 原子递增元素计数
func (dict *ConcurrentDict) addCount() int32 {
	return atomic.AddInt32(&dict.count, 1)
}

// decreaseCount 原子递减元素计数
func (dict *ConcurrentDict) decreaseCount() int32 {
	return atomic.AddInt32(&dict.count, -1)
}

// Put 插入或更新一个 key
func (dict *ConcurrentDict) Put(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	s.mx.Lock()
	defer s.mx.Unlock()
	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 0
	}
	dict.addCount()
	s.m[key] = val
	return 1
}

// PutWithLock 在外部已加锁的情况下插入或更新 key
func (dict *ConcurrentDict) PutWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 0
	}
	dict.addCount()
	s.m[key] = val
	return 1
}

// PutIfAbsent 仅当 key 不存在时插入
func (dict *ConcurrentDict) PutIfAbsent(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	s.mx.Lock()
	defer s.mx.Unlock()
	if _, ok := s.m[key]; ok {
		return 0
	}
	s.m[key] = val
	dict.addCount()
	return 1
}

// PutIfAbsentWithLock 在外部已加锁的情况下执行 PutIfAbsent
func (dict *ConcurrentDict) PutIfAbsentWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	if _, ok := s.m[key]; ok {
		return 0
	}
	s.m[key] = val
	dict.addCount()
	return 1
}

// PutIfExists 仅当 key 已存在时更新
func (dict *ConcurrentDict) PutIfExists(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	s.mx.Lock()
	defer s.mx.Unlock()
	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}
	return 0
}

// PutIfExistsWithLock 在外部已加锁的情况下执行 PutIfExists
func (dict *ConcurrentDict) PutIfExistsWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}
	return 0
}

// Remove 删除指定 key 并返回旧值
func (dict *ConcurrentDict) Remove(key string) (val interface{}, result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	s.mx.Lock()
	defer s.mx.Unlock()
	if val, ok := s.m[key]; ok {
		delete(s.m, key)
		dict.decreaseCount()
		return val, 1
	}
	return nil, 0
}

// RemoveWithLock 在外部已加锁的情况下删除 key
func (dict *ConcurrentDict) RemoveWithLock(key string) (val interface{}, result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	if val, ok := s.m[key]; ok {
		delete(s.m, key)
		dict.decreaseCount()
		return val, 1
	}
	return val, 0
}

// ForEach 遍历字典并对每个元素执行回调
func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	if dict == nil {
		panic("dict is nil")
	}
	for _, s := range dict.table {
		s.mx.RLock()
		f := func() bool {
			defer s.mx.RUnlock()
			for key, value := range s.m {
				if !consumer(key, value) {
					return false
				}
			}
			return true
		}
		if !f() {
			break
		}
	}
}

// Keys 返回所有 key 的切片
func (dict *ConcurrentDict) Keys() []string {
	keys := make([]string, 0, dict.Len())
	dict.ForEach(func(key string, val interface{}) bool {
		keys = append(keys, key)
		return true
	})
	return keys
}

// RandomKey 从分片中随机返回一个 key
func (shard *shard) RandomKey() string {
	if shard == nil {
		panic("dict is nil")
	}
	shard.mx.RLock()
	defer shard.mx.RUnlock()
	for key := range shard.m {
		return key
	}
	return ""
}

// RandomKeys 随机返回指定数量的 key（允许重复）
func (dict *ConcurrentDict) RandomKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}
	shardCount := len(dict.table)
	result := make([]string, limit)
	for i := 0; i < limit; {
		shardIndex := rand.IntN(shardCount)
		s := dict.getShard(uint32(shardIndex))
		key := s.RandomKey()
		if key != "" {
			result[i] = key
			i++
		}
	}
	return result
}

// RandomDistinctKeys 随机返回指定数量的不重复 key
func (dict *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}
	shardCount := len(dict.table)
	result := make(map[string]struct{})
	for len(result) < limit {
		shardIndex := rand.IntN(shardCount)
		s := dict.getShard(uint32(shardIndex))
		key := s.RandomKey()
		if key != "" {
			result[key] = struct{}{}
		}
	}
	arr := make([]string, 0, limit)
	for k := range result {
		arr = append(arr, k)
	}
	return arr
}

// Clear 清空字典中的所有数据
func (dict *ConcurrentDict) Clear() {
	for _, s := range dict.table {
		s.mx.Lock()
		if len(s.m) > 0 {
			atomic.AddInt32(&dict.count, -int32(len(s.m)))
			s.m = make(map[string]interface{})
		}
		s.mx.Unlock()
	}
}

// toLockIndices 将 key 转换为排序后的分片索引
func (dict *ConcurrentDict) toLockIndices(keys []string, reverse bool) []uint32 {
	indexMap := make(map[uint32]struct{})
	for _, key := range keys {
		indexMap[dict.spread(key)] = struct{}{}
	}
	indices := make([]uint32, 0, len(indexMap))
	for index := range indexMap {
		indices = append(indices, index)
	}
	sort.Slice(indices, func(i, j int) bool {
		if !reverse {
			return indices[i] < indices[j]
		}
		return indices[i] > indices[j]
	})
	return indices
}

// RWLocks 同时对写 key 和读 key 进行加锁
func (dict *ConcurrentDict) RWLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := dict.toLockIndices(keys, false)
	writeIndexSet := make(map[uint32]struct{})
	for _, k := range writeKeys {
		writeIndexSet[dict.spread(k)] = struct{}{}
	}
	for _, index := range indices {
		if _, ok := writeIndexSet[index]; ok {
			dict.table[index].mx.Lock()
		} else {
			dict.table[index].mx.RLock()
		}
	}
}

// RWUnLocks 解锁 RWLocks 中加的所有锁
func (dict *ConcurrentDict) RWUnLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := dict.toLockIndices(keys, true)
	writeIndexSet := make(map[uint32]struct{})
	for _, k := range writeKeys {
		writeIndexSet[dict.spread(k)] = struct{}{}
	}
	for _, index := range indices {
		if _, ok := writeIndexSet[index]; ok {
			dict.table[index].mx.Unlock()
		} else {
			dict.table[index].mx.RUnlock()
		}
	}
}

// stringsToBytes 将字符串切片转换为字节切片
func stringsToBytes(strSlice []string) [][]byte {
	byteSlice := make([][]byte, len(strSlice))
	for i, str := range strSlice {
		byteSlice[i] = []byte(str)
	}
	return byteSlice
}

// DictScan 实现类似 Redis SCAN 的游标遍历
func (dict *ConcurrentDict) DictScan(cursor int, count int, pattern string) ([][]byte, int) {
	size := dict.Len()
	result := make([][]byte, 0)

	if pattern == "*" && count >= size {
		return stringsToBytes(dict.Keys()), 0
	}
	matchKey, err := wildcard.CompilePattern(pattern)
	if err != nil {
		return result, -1
	}
	shardCount := len(dict.table)
	shardIndex := cursor
	for shardIndex < shardCount {
		s := dict.table[shardIndex]
		s.mx.RLock()
		for key := range s.m {
			if pattern == "*" || matchKey.IsMatch(key) {
				result = append(result, []byte(key))
			}
		}
		s.mx.RUnlock()
		shardIndex++
		if len(result) >= count {
			return result, shardIndex
		}
	}
	return result, 0
}
