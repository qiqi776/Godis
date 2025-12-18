package sortedset

import (
	"strconv"

	"github.com/hdt3213/godis/lib/wildcard"
)

type SortedSet struct {
	dict map[string]*Element
	skiplist *skiplist
}

func Make() *SortedSet {
	return &SortedSet{
		dict: make(map[string]*Element),
		skiplist: makeskiplist(),
	}
}

func (ss *SortedSet) Add(member string, score float64) bool {
	elem, ok := ss.dict[member]
	ss.dict[member] = &Element{
		Member: member,
		Score: score,
	}
	if ok {
		if score != elem.Score {
			ss.skiplist.isremove(member, elem.Score)
			ss.skiplist.insert(member, score)
		}
		return false
	}
	ss.skiplist.insert(member, score)
	return true
}

func (ss *SortedSet) Len() int64 {
	return int64(len(ss.dict))
}

func (ss *SortedSet) Get(member string) (elem *Element, ok bool) {
	elem, ok = ss.dict[member]
	if !ok {
		return nil, false
	}
	return elem, true
}

func (ss *SortedSet) Remove(member string) bool {
	elem, ok := ss.dict[member]
	if ok {
		ss.skiplist.isremove(member, elem.Score)
		delete(ss.dict, member)
		return true
	}
	return false
}

func (ss *SortedSet) GetRank(member string, desc bool) (rank int64) {
	elem, ok := ss.dict[member]
	if !ok {
		return -1
	}
	r := ss.skiplist.getRank(member, elem.Score)
	if desc {
		r = ss.skiplist.length - r -1
	}
	return r
}

func (ss * SortedSet) ForEachByRank(start int64, stop int64, desc bool, consumer func(elem *Element) bool) {
	size := ss.Len()
	if start < 0 || start >= size {
		panic("illegal start " + strconv.FormatInt(start, 10))
	}
	if stop < start || stop > size {
		panic("illegal end " + strconv.FormatInt(stop, 10))
	}

	var node *node
	if desc {
		node = ss.skiplist.tail
		if start > 0 {
			node = ss.skiplist.getByRank(size - 1 - start)
		}
	} else {
		node = ss.skiplist.header.level[0].forward
		if start > 0 {
			node = ss.skiplist.getByRank(start)
		}
	}

	sliceSize := int(stop - start) + 1
	for i := 0; i < sliceSize; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}
}

func (ss *SortedSet) RangeByRank(start int64, stop int64, desc bool) []*Element {
	size := ss.Len()
	if start < 0 {
		start = 0
	}
	if start >= size && stop < start {
		return []*Element{}
	}
	if stop >= size {
		stop = size - 1
	}
	sliceSize := int(stop - start) + 1
	slice := make([]*Element, sliceSize)
	i := 0
	ss.ForEachByRank(start, stop, desc, func(elem *Element) bool {
		slice[i] = elem
		i++
		return true
	})
	return slice
}

func (ss *SortedSet) RangeCount(min Border, max Border) int64 {
    first := ss.skiplist.getFirstInRange(min, max)
    if first == nil {
        return 0
    }

    last := ss.skiplist.getLastInRange(min, max)
    if last == nil {
        return 0
    }

    rank1 := ss.skiplist.getRank(first.Member, first.Score)
    rank2 := ss.skiplist.getRank(last.Member, last.Score)

    return rank2 - rank1 + 1
}

func (ss *SortedSet) ForEach(min Border, max Border, offset int64, limit int64, desc bool, consumer func(element *Element) bool) {
	var node *node 
	if desc {
		node = ss.skiplist.getLastInRange(min, max)
	} else {
		node = ss.skiplist.getFirstInRange(min, max)
	}

	for node != nil && offset > 0 {
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		offset--
	}

	for i := 0; (i < int(limit) || limit < 0) && node != nil; i++ {
		if desc {
			if !min.less(&node.Element) {
				break
			}
		} else {
			if !max.greater(&node.Element) {
				break
			}
		}
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}
}

func (ss *SortedSet) Range(min Border, max Border, offset int64, limit int64, desc bool) []*Element {
	if limit == 0 || offset < 0 {
		return make([]*Element, 0)
	}
	slice := make([]*Element, 0)
	ss.ForEach(min, max, offset, limit, desc, func(elem *Element) bool {
		slice = append(slice, elem)
		return true
	})
	return slice
}

func (ss *SortedSet) RemoveRange(min Border, max Border) int64 {
	removed := ss.skiplist.RemoveRange(min, max, 0)
	for _, elem := range removed {
		delete(ss.dict, elem.Member)
	}
	return int64(len(removed))
}

func (ss *SortedSet) PopMin(count int) []*Element {
	first := ss.skiplist.getFirstInRange(scoreNegativeInfBorder, scorePositiveInfBorder)
	if first == nil {
		return nil
	}
	border := &ScoreBorder{
		Value:   first.Score,
		Exclude: false,
	}
	removed := ss.skiplist.RemoveRange(border, scorePositiveInfBorder, count)
	for _, elem := range removed {
		delete(ss.dict, elem.Member)
	}
	return removed
}

func (ss *SortedSet) RemoveByRank(start int64, stop int64) int64 {
	removed := ss.skiplist.RemoveRangeByRank(start, stop)
	for _, elem := range removed {
		delete(ss.dict, elem.Member)
	}
	return int64(len(removed))
}

func (ss *SortedSet) ZSetScan(cursor int, count int, pattern string) ([][]byte, int) {
	result := make([][]byte, 0)
	matchkey, err := wildcard.CompilePattern(pattern)
	if err != nil {
		return result, -1
	}
	for k := range ss.dict {
		if pattern == "*" || matchkey.IsMatch(k) {
			elem, exists := ss.dict[k]
			if !exists {
				continue
			}
			result = append(result, []byte(k))
			result = append(result, []byte(strconv.FormatFloat(elem.Score, 'f', 10, 64)))
		}
	}
	return result, 0
}