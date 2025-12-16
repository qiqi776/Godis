package sortedset

import (
	"math/bits"
	"math/rand"
)

const (
	maxLevel = 32
)

type Element struct {
	Member string
	Score  float64
}
type node struct {
	Element
	backward *node
	level    []struct {
		forward *node
		span    int64
	}
}

type skiplist struct {
	header, tail *node
	length       int64
	level        int
}

func makenode(level int, score float64, member string) *node {
	n := &node{
		Element: Element{
			Score:  score,
			Member: member,
		},
		level: make([]struct {
			forward *node
			span    int64
		}, level),
	}
	return n
}

func makeskiplist() *skiplist {
	return &skiplist{
		level:  1,
		length: 0,
		header: makenode(maxLevel, 0, ""),
	}
}

func randomLevel() int {
	rd := rand.Uint64()
	level := 1 + (bits.TrailingZeros64(rd) >> 1)
	if level > maxLevel {
		return maxLevel
	}
	return level
}

func (sl *skiplist) insert(member string, score float64) *node {
	update := make([]*node, maxLevel)
	rank := make([]int64, maxLevel)
	node := sl.header

	// 查找插入位置
	for i := sl.level - 1; i >= 0; i-- {
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for node.level[i].forward != nil && (node.level[i].forward.Score < score ||
			(node.level[i].forward.Score == score && node.level[i].forward.Member < member)) {
			rank[i] += node.level[i].span
			node = node.level[i].forward
		}
		update[i] = node
	}

	// 检查Member是否已存在
	nextNode := node.level[0].forward
	if nextNode != nil && nextNode.Member == member {
		sl.removeNode(nextNode, update)
		node = sl.header
		for i := sl.level - 1; i >= 0; i-- {
			if i == sl.level-1 {
				rank[i] = 0
			} else {
				rank[i] = rank[i+1]
			}
			for node.level[i].forward != nil && (node.level[i].forward.Score < score ||
				(node.level[i].forward.Score == score && node.level[i].forward.Member < member)) {
				rank[i] += node.level[i].span
				node = node.level[i].forward
			}
			update[i] = node
		}
	}

	// 生成新层高
	level := randomLevel()
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			rank[i] = 0
			update[i] = sl.header
			update[i].level[i].span = sl.length
		}
		sl.level = level
	}

	// 插入新节点
	node = makenode(level, score, member)
	for i := 0; i < level; i++ {
		node.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = node
		
		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// 更新高层 span
	for i := level; i < sl.level; i++ {
		update[i].level[i].span++
	}

	// 设置 backward
	if update[0] == sl.header {
		node.backward = nil
	} else {
		node.backward = update[0]
	}
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node
	} else {
		sl.tail = node
	}
	sl.length++
	return node
}

func (sl *skiplist) removeNode(node *node, update []*node) {
	for i := 0; i < sl.level; i++ {
		if update[i].level[i].forward == node {
			update[i].level[i].span += node.level[i].span - 1
			update[i].level[i].forward = node.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node.backward
	} else {
		sl.tail = node.backward
	}
	for sl.level > 1 && sl.header.level[sl.level-1].forward == nil {
		sl.level--
	}
	sl.length--
}

func (sl *skiplist) isremove(member string, score float64) bool {
	update := make([]*node, maxLevel)
	node := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil &&
			(node.level[i].forward.Score < score ||
				(node.level[i].forward.Score == score &&
					node.level[i].forward.Member < member)) {
			node = node.level[i].forward
		}
		update[i] = node
	}
	node = node.level[0].forward
	if node != nil && score == node.Score && node.Member == member {
		sl.removeNode(node, update)
		return true
	}
	return false
}

func (sl *skiplist) getRank(member string, score float64) int64 {
	var rank int64 = 0
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && (x.level[i].forward.Score < score ||
			(x.level[i].forward.Score == score && x.level[i].forward.Member <= member)) {
			rank += x.level[i].span
			x = x.level[i].forward
		}
		if x.Member == member {
			return rank - 1
		}
	}
	return -1
}

func (sl *skiplist) getByRank(rank int64) *node {
	if rank < 0 || rank >= sl.length {
		return nil
	}
	var i int64 = 0
	n := sl.header
	target := rank + 1
	for level := sl.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && (i+n.level[level].span) <= target {
			i += n.level[level].span
			n = n.level[level].forward
		}
		if i == target {
			return n
		}
	}
	return nil
}

// 判断元素是否存在范围内
func (sl *skiplist) hasInRange(min Border, max Border) bool {
	if min.isIntersected(max) {
		return false
	}
	n := sl.tail
	if n == nil || !min.less(&n.Element) {
		return false
	}
	n = sl.header.level[0].forward
	if n == nil || !max.greater(&n.Element) {
		return false
	}
	return true
}

// 查找范围内第一个节点
func (sl *skiplist) getFirstInRange(min Border, max Border) *node {
	if !sl.hasInRange(min, max) {
		return nil
	}
	n := sl.header
	for level := sl.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && !min.less(&n.level[level].forward.Element) {
			n = n.level[level].forward
		}
	}
	n = n.level[0].forward
	if !max.greater(&n.Element) {
		return nil
	}
	return n
}

// 查找范围内最后一个节点
func (sl *skiplist) getLastInRange(min Border, max Border) *node {
	if !sl.hasInRange(min, max) {
		return nil
	}
	n := sl.header
	for level := sl.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && max.greater(&n.level[level].forward.Element) {
			n = n.level[level].forward
		}
	}
	if !min.less(&n.Element) {
		return nil
	}
	return n
}

// 删除范围内的元素
func (sl *skiplist) RemoveRange(min Border, max Border, limit int) (removed []*Element) {
	update := make([]*node, maxLevel)
	removed = make([]*Element, 0)
	node := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil {
			if min.less(&node.level[i].forward.Element) {
				break
			}
			node = node.level[i].forward
		}
		update[i] = node
	}
	node = node.level[0].forward
	for node != nil {
		if !max.greater(&node.Element) {
			break
		}
		next := node.level[0].forward
		removedElement := node.Element
		removed = append(removed, &removedElement)
		sl.removeNode(node, update)
		if limit > 0 && len(removed) == limit {
			break
		}
		node = next
	}
	return removed
}

// 删除排名范围内的元素
func (sl *skiplist) RemoveRangeByRank(start int64, stop int64) (removed []*Element) {
	var i int64 = 0
	update := make([]*node, maxLevel)
	removed = make([]*Element, 0)

	node := sl.header
	for level := sl.level - 1; level >= 0; level-- {
		for node.level[level].forward != nil && (i+node.level[level].span) <= start {
			i += node.level[level].span
			node = node.level[level].forward
		}
		update[level] = node
	}

	node = node.level[0].forward
	for node != nil && i < stop {
		next := node.level[0].forward
		removedElement := node.Element
		removed = append(removed, &removedElement)
		sl.removeNode(node, update)
		node = next
		i++
	}
	return removed
}