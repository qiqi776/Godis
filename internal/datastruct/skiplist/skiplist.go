package skiplist

import (
	"math/rand"
	"time"
)

const (
	maxLevel = 16
	p        = 0.25
)

type Element struct {
	Member string
	Score  float64
}

type node struct {
	element  Element
	forwards []*node
	backward *node
}

type SkipList struct {
	header *node
	tail   *node
	level  int
	length int
	rnd    *rand.Rand
}

func New() *SkipList {
	return &SkipList{
		header: &node{
			forwards: make([]*node, maxLevel),
		},
		level: 1,
		rnd:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (sl *SkipList) Len() int {
	return sl.length
}

func (sl *SkipList) Insert(member string, score float64) {
	update := make([]*node, maxLevel)
	x := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for x.forwards[i] != nil && less(x.forwards[i].element, score, member) {
			x = x.forwards[i]
		}
		update[i] = x
	}

	level := sl.randomLevel()
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			update[i] = sl.header
		}
		sl.level = level
	}

	n := &node{
		element: Element{
			Member: member,
			Score:  score,
		},
		forwards: make([]*node, level),
	}

	for i := 0; i < level; i++ {
		n.forwards[i] = update[i].forwards[i]
		update[i].forwards[i] = n
	}

	if update[0] == sl.header {
		n.backward = nil
	} else {
		n.backward = update[0]
	}

	if n.forwards[0] != nil {
		n.forwards[0].backward = n
	} else {
		sl.tail = n
	}

	sl.length++
}

func (sl *SkipList) Remove(member string, score float64) bool {
	update := make([]*node, maxLevel)
	x := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for x.forwards[i] != nil && less(x.forwards[i].element, score, member) {
			x = x.forwards[i]
		}
		update[i] = x
	}

	target := x.forwards[0]
	if target == nil || target.element.Score != score || target.element.Member != member {
		return false
	}

	for i := 0; i < sl.level; i++ {
		if update[i].forwards[i] == target {
			update[i].forwards[i] = target.forwards[i]
		}
	}

	if target.forwards[0] != nil {
		target.forwards[0].backward = target.backward
	} else {
		sl.tail = target.backward
	}

	for sl.level > 1 && sl.header.forwards[sl.level-1] == nil {
		sl.level--
	}

	sl.length--
	return true
}

func (sl *SkipList) Range(start, stop int) []Element {
	if start < 0 || stop < start || start >= sl.length {
		return []Element{}
	}
	if stop >= sl.length {
		stop = sl.length - 1
	}

	x := sl.header.forwards[0]
	for i := 0; i < start && x != nil; i++ {
		x = x.forwards[0]
	}

	out := make([]Element, 0, stop-start+1)
	for i := start; i <= stop && x != nil; i++ {
		out = append(out, x.element)
		x = x.forwards[0]
	}
	return out
}

func (sl *SkipList) randomLevel() int {
	level := 1
	for level < maxLevel && sl.rnd.Float64() < p {
		level++
	}
	return level
}

func less(e Element, score float64, member string) bool {
	if e.Score < score {
		return true
	}
	if e.Score > score {
		return false
	}
	return e.Member < member
}