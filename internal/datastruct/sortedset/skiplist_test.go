package sortedset

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestSkiplist_Insert(t *testing.T) {
	sl := makeskiplist()
	node1 := sl.insert("a", 1.0)
	node2 := sl.insert("b", 2.0)
	node3 := sl.insert("c", 3.0)
	assert.Equal(t, node1.Member, "a")
	assert.Equal(t, node2.Member, "b")
	assert.Equal(t, node3.Member, "c")
	assert.Equal(t, node1.Score, 1.0)
	assert.Equal(t, node2.Score, 2.0)
	assert.Equal(t, node3.Score, 3.0)
	assert.Equal(t, sl.length, int64(3))
}

func TestSkiplist_RemoveNode(t *testing.T) {
	sl := makeskiplist()
	sl.insert("a", 1.0)
	sl.insert("b", 2.0)
	sl.insert("c", 3.0)
	removed := sl.isremove("b", 2.0)
	assert.True(t, removed)
	assert.Equal(t, sl.length, int64(2))
	node := sl.getByRank(0)
	assert.NotNil(t, node)
	assert.Equal(t, node.Member, "a")
	node = sl.getByRank(1)
	assert.NotNil(t, node)
	assert.Equal(t, node.Member, "c")
}

func TestSkiplist_RemoveRangeByRank(t *testing.T) {
	sl := makeskiplist()
	sl.insert("a", 1.0)
	sl.insert("b", 2.0)
	sl.insert("c", 3.0)
	removed := sl.RemoveRangeByRank(0, 1) 
	assert.Len(t, removed, 2)
	assert.Equal(t, removed[0].Member, "a")
	assert.Equal(t, removed[1].Member, "b")
	assert.Equal(t, sl.length, int64(1))
}

func TestSkiplist_RemoveRange(t *testing.T) {
	sl := makeskiplist()
	sl.insert("a", 1.0)
	sl.insert("b", 2.0)
	sl.insert("c", 3.0)
	sl.insert("d", 4.0)
	removed := sl.RemoveRange(&ScoreBorder{Value: 1.5, Exclude: true}, &ScoreBorder{Value: 3.5, Exclude: true}, 0)
	assert.Len(t, removed, 2)
	assert.Equal(t, removed[0].Member, "b")
	assert.Equal(t, removed[1].Member, "c")
	assert.Equal(t, sl.length, int64(2))
}

func TestSkiplist_GetFirstInRange(t *testing.T) {
	sl := makeskiplist()
	sl.insert("a", 1.0)
	sl.insert("b", 2.0)
	sl.insert("c", 3.0)
	sl.insert("d", 4.0)
	node := sl.getFirstInRange(&ScoreBorder{Value: 1.5, Exclude: true}, &ScoreBorder{Value: 3.5, Exclude: true})
	assert.NotNil(t, node)
	assert.Equal(t, node.Member, "b")
}

func TestSkiplist_GetLastInRange(t *testing.T) {
	sl := makeskiplist()
	sl.insert("a", 1.0)
	sl.insert("b", 2.0)
	sl.insert("c", 3.0)
	sl.insert("d", 4.0)
	node := sl.getLastInRange(&ScoreBorder{Value: 1.5, Exclude: true}, &ScoreBorder{Value: 3.5, Exclude: true})
	assert.NotNil(t, node)
	assert.Equal(t, node.Member, "c")
}

func TestSkiplist_GetRank(t *testing.T) {
	sl := makeskiplist()
	sl.insert("a", 1.0)
	sl.insert("b", 2.0)
	sl.insert("c", 3.0)
	sl.insert("d", 4.0)
	rank := sl.getRank("b", 2.0)
	assert.Equal(t, rank, int64(1))
	rank = sl.getRank("d", 4.0)
	assert.Equal(t, rank, int64(3))
}

func TestSkiplist_RandomLevel(t *testing.T) {
	levelCount := make(map[int]int)
	for i := 0; i < 10000; i++ {
		level := randomLevel()
		levelCount[level]++
	}
	for i := 1; i <= maxLevel; i++ {
		t.Logf("Level %d count: %d\n", i, levelCount[i])
	}
}
