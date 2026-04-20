package zset

import (
	"godis/internal/datastruct/dict"
	"godis/internal/datastruct/skiplist"
)

type Element = skiplist.Element

type ZSet struct {
	dict *dict.Dict
	sl   *skiplist.SkipList
}

func New() *ZSet {
	return &ZSet{
		dict: dict.New(),
		sl:   skiplist.New(),
	}
}

func (z *ZSet) Len() int {
	return z.dict.Len()
}

func (z *ZSet) Add(member string, score float64) (int64, bool) {
	raw, ok := z.dict.Get(member)
	if ok {
		oldScore := raw.(float64)
		if oldScore == score {
			return 0, false
		}
		z.sl.Remove(member, oldScore)
		z.dict.Put(member, score)
		z.sl.Insert(member, score)
		return 0, true
	}

	z.dict.Put(member, score)
	z.sl.Insert(member, score)
	return 1, true
}

func (z *ZSet) Remove(members ...string) int64 {
	var removed int64
	for _, member := range members {
		raw, ok := z.dict.Get(member)
		if !ok {
			continue
		}
		score := raw.(float64)
		z.sl.Remove(member, score)
		z.dict.Remove(member)
		removed++
	}
	return removed
}

func (z *ZSet) Score(member string) (float64, bool) {
	raw, ok := z.dict.Get(member)
	if !ok {
		return 0, false
	}
	return raw.(float64), true
}

func (z *ZSet) Range(start, stop int) []Element {
	return z.sl.Range(start, stop)
}
