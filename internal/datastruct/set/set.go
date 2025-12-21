package set

import (
	"godis/internal/datastruct/dict"
	"godis/pkg/wildcard"
)

type Set struct {
	dict dict.Dict
}

// Make 创建 Set，基于字典实现，无重复
func Make(members ...string) *Set {
	set := &Set{
		dict: dict.MakeSimple(),
	}
	for _, member := range members {
		set.Add(member)
	}
	return set
}

// Add 向集合中加入一个元素
func (set *Set) Add(val string) int {
	return set.dict.Put(val, nil)
}

// Remove 向集合中删除一个元素
func (set *Set) Remove(val string) int {
	_, result := set.dict.Remove(val)
	return result
}

// Has 判断元素是否存在
func (set *Set) Has(val string) bool {
	_, exists := set.dict.Get(val)
	return exists
}

// Len 返回集合元素数量
func (set *Set) Len() int {
	if set == nil || set.dict == nil {
		return 0
	}
	return set.dict.Len()
}

// ToSlice 将集合中的元素转换为切片
func (set *Set) ToSlice() []string {
	slice := make([]string, 0, set.Len())
	set.dict.ForEach(func(key string, val interface{}) bool {
		slice = append(slice, key)
		return true
	})
	return slice
}

// ForEach 遍历集合中所有成员，并执行传入函数
func (set *Set) ForEach(consumer func(member string) bool) {
	if set == nil || set.dict == nil {
		return
	}
	set.dict.ForEach(func(key string, val interface{}) bool {
		return consumer(key)
	})
}

// ShallowCopy 创建一个浅拷贝的新 Set
func (set *Set) ShallowCopy() *Set {
	result := Make()
	set.ForEach(func(member string) bool {
		result.Add(member)
		return true
	})
	return result
}

// Intersect 计算多个集合的交集
func Intersect(sets ...*Set) *Set {
	result := Make()
	if len(sets) == 0 {
		return result
	}
	minLenSet := sets[0]
	for _, set := range sets {
		if set.Len() < minLenSet.Len() {
			minLenSet = set
		}
	}
	minLenSet.ForEach(func(member string) bool {
		isMemberOfAll := true
		for _, set := range sets {
			if set == minLenSet {
				continue
			}
			if !set.Has(member) {
				isMemberOfAll = false
				break
			}
		}
		if isMemberOfAll {
				result.Add(member)
			}
			return true
	})
	return result
}

// Union 计算多个集合的并集
func Union(sets ...*Set) *Set {
	result := Make()
	for _, set := range sets {
		set.ForEach(func(member string) bool {
			result.Add(member)
			return true
		})
	}
	return result
}

// Diff 差集 第一个集合有的，但后面集合都没有的
func Diff(sets ...*Set) *Set {
    result := Make()
    if len(sets) == 0 {
        return result
    }
    sets[0].ForEach(func(member string) bool {
        inOther := false
        for i := 1; i < len(sets); i++ {
            if sets[i].Has(member) {
                inOther = true
                break
            }
        }
        if !inOther {
            result.Add(member)
        }
        return true
    })
    return result
}

// RandomMembers 返回指定数量的随机成员
func (set *Set) RandomMembers(limit int) []string {
	if set == nil || set.dict == nil {
		return nil
	}
	return set.dict.RandomKeys(limit)
}

// RandomDistinctMembers 返回指定数量的不重复随机成员
func (set *Set) RandomDistinctMembers(limit int) []string {
	return set.dict.RandomDistinctKeys(limit)
}

// SetScan 扫描集合成员并按模式匹配返回
func (set *Set) SetScan(cursor int, count int, pattern string) ([][]byte, int) {
	result := make([][]byte, 0)
	matchKey, err := wildcard.CompilePattern(pattern)
	if err != nil {
		return result, 0
	}
	set.ForEach(func(member string) bool {
		if pattern == "*" || matchKey.IsMatch(member) {
			result = append(result, []byte(member))
		}
		return true
	})
	return result, 0
}
