package dict

import "godis/pkg/wildcard"

type SimpleDict struct {
	m map[string]interface{}
}

func MakeSimple() *SimpleDict {
	return &SimpleDict{
		m: make(map[string]interface{}),
	}
}

func (dict *SimpleDict) Get(key string) (val interface{}, exists bool) {
	val, ok := dict.m[key]
	return val, ok
}

// Len 返回字典的长度
func (dict *SimpleDict) Len() int {
	if dict.m == nil {
		panic("m is nil")
	}
	return len(dict.m)
}

// Put 将键值对放入字典，返回新增的键值对数量
func (dict *SimpleDict) Put(key string, val interface{}) (result int) {
	_, existed := dict.m[key]
	dict.m[key] = val
	if existed {
		return 0
	}
	return 1
}

// PutIfAbsent 如果键不存在则放入值，返回更新的键值对数量
func (dict *SimpleDict) PutIfAbsent(key string, val interface{}) (result int) {
	_, existed := dict.m[key]
	if existed {
		return 0
	}
	dict.m[key] = val
	return 1
}

// PutIfExists 如果键已存在则更新值，返回更新的键值对数量
func (dict *SimpleDict) PutIfExists(key string, val interface{}) (result int) {
	_, existed := dict.m[key]
	if existed {
		dict.m[key] = val
		return 1
	}
	return 0
}

// Remove 删除指定键，返回被删除的键值对数量
func (dict *SimpleDict) Remove(key string) (val interface{}, result int) {
	val, existed := dict.m[key]
	delete(dict.m, key)
	if existed {
		return val, 1
	}
	return nil, 0
}

// Keys 返回字典中所有的键
func (dict *SimpleDict) Keys() []string {
	result := make([]string, len(dict.m))
	i := 0
	for k := range dict.m {
		result[i] = k
		i++
	}
	return result
}

// ForEach 遍历字典
func (dict *SimpleDict) ForEach(consumer Consumer) {
	for k, v := range dict.m {
		if !consumer(k, v) {
			break
		}
	}
}

// RandomKeys 随机返回指定数量的键（可能包含重复）
func (dict *SimpleDict) RandomKeys(limit int) []string {
	result := make([]string, limit)
	for i := 0; i < limit; i++ {
		for k := range dict.m {
			result[i] = k
			break
		}
	}
	return result
}

// RandomDistinctKeys 随机返回指定数量的唯一键（不重复）
func (dict *SimpleDict) RandomDistinctKeys(limit int) []string {
	size := limit
	if size > len(dict.m) {
		size = len(dict.m)
	}
	result := make([]string, size)
	i := 0
	for k := range dict.m {
		if i == size {
			break
		}
		result[i] = k
		i++
	}
	return result
}

// Clear 清空字典中的所有键值对
func (dict *SimpleDict) Clear() {
	*dict = *MakeSimple()
}

// DictScan 按照游标、分页数量和匹配模式扫描字典（用于实现类似 Redis 的 SCAN 命令）
func (dict *SimpleDict) DictScan(cursor int, count int, pattern string) ([][]byte, int) {
	result := make([][]byte, 0)
	matchKey, err := wildcard.CompilePattern(pattern)
	if err != nil {
		return result, -1
	}
	for k := range dict.m {
		if pattern == "*" || matchKey.IsMatch(k) {
			raw, exists := dict.Get(k)
			if !exists {
				continue
			}
			result = append(result, []byte(k))
			result = append(result, raw.([]byte))
		}
	}
	return result, 0
}