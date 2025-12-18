package sortedset

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSortedSet_AddAndGet(t *testing.T) {
	ss := Make()

	// Test Add
	assert.True(t, ss.Add("a", 1.0))
	assert.True(t, ss.Add("b", 2.0))
	
	// Test Update Score
	assert.False(t, ss.Add("a", 1.5))

	// Test Len
	assert.Equal(t, int64(2), ss.Len())

	// Test Get
	elem, ok := ss.Get("a")
	assert.True(t, ok)
	assert.Equal(t, 1.5, elem.Score)

	elem, ok = ss.Get("c")
	assert.False(t, ok)
	assert.Nil(t, elem)
}

func TestSortedSet_Remove(t *testing.T) {
	ss := Make()
	ss.Add("a", 1.0)
	ss.Add("b", 2.0)

	assert.True(t, ss.Remove("a"))
	assert.Equal(t, int64(1), ss.Len())
	
	_, ok := ss.Get("a")
	assert.False(t, ok)

	assert.False(t, ss.Remove("c"))
}

func TestSortedSet_GetRank(t *testing.T) {
	ss := Make()
	ss.Add("a", 10.0)
	ss.Add("b", 20.0)
	ss.Add("c", 30.0)

	// Rank (Asc)
	assert.Equal(t, int64(0), ss.GetRank("a", false))
	assert.Equal(t, int64(1), ss.GetRank("b", false))
	assert.Equal(t, int64(2), ss.GetRank("c", false))

	// Rank (Desc)
	assert.Equal(t, int64(2), ss.GetRank("a", true))
	assert.Equal(t, int64(1), ss.GetRank("b", true))
	assert.Equal(t, int64(0), ss.GetRank("c", true))
}

func TestSortedSet_RangeByRank(t *testing.T) {
	ss := Make()
	ss.Add("a", 1.0)
	ss.Add("b", 2.0)
	ss.Add("c", 3.0)
	ss.Add("d", 4.0)
	ss.Add("e", 5.0)

	// Test Inclusive Range [0, 2] -> a, b, c
	res := ss.RangeByRank(0, 2, false)
	assert.Len(t, res, 3)
	assert.Equal(t, "a", res[0].Member)
	assert.Equal(t, "c", res[2].Member)

	// Test Desc Range [0, 1] -> e, d
	res = ss.RangeByRank(0, 1, true)
	assert.Len(t, res, 2)
	assert.Equal(t, "e", res[0].Member)
	assert.Equal(t, "d", res[1].Member)
}

func TestSortedSet_Range(t *testing.T) {
	ss := Make()
	ss.Add("a", 1.0)
	ss.Add("b", 2.0)
	ss.Add("c", 3.0)
	ss.Add("d", 4.0)

	min := &ScoreBorder{Value: 1.5, Exclude: false}
	max := &ScoreBorder{Value: 3.5, Exclude: false}

	// Range [1.5, 3.5] -> b(2.0), c(3.0)
	res := ss.Range(min, max, 0, -1, false)
	
	// 如果之前有双重迭代的Bug，这里可能会漏掉元素或者长度不对
	assert.Len(t, res, 2)
	assert.Equal(t, "b", res[0].Member)
	assert.Equal(t, "c", res[1].Member)

	// Test Limit
	res = ss.Range(min, max, 0, 1, false)
	assert.Len(t, res, 1)
	assert.Equal(t, "b", res[0].Member)
}

func TestSortedSet_RemoveRange(t *testing.T) {
	ss := Make()
	ss.Add("a", 1.0)
	ss.Add("b", 2.0)
	ss.Add("c", 3.0)
	ss.Add("d", 4.0)

	min := &ScoreBorder{Value: 1.5, Exclude: false}
	max := &ScoreBorder{Value: 3.5, Exclude: false}

	removedCount := ss.RemoveRange(min, max)
	assert.Equal(t, int64(2), removedCount) // Remove b, c

	assert.Equal(t, int64(2), ss.Len())
	_, ok := ss.Get("b")
	assert.False(t, ok)
}

func TestSortedSet_RemoveByRank(t *testing.T) {
	ss := Make()
	ss.Add("a", 1.0)
	ss.Add("b", 2.0)
	ss.Add("c", 3.0)

	// Remove rank 0 and 1 (a and b)
	removedCount := ss.RemoveByRank(0, 1)
	assert.Equal(t, int64(2), removedCount)
	
	assert.Equal(t, int64(1), ss.Len())
	res := ss.RangeByRank(0, 10, false)
	assert.Equal(t, "c", res[0].Member)
}

func TestSortedSet_PopMin(t *testing.T) {
	ss := Make()
	ss.Add("a", 1.0)
	ss.Add("b", 2.0)
	ss.Add("c", 3.0)

	popped := ss.PopMin(2)
	assert.Len(t, popped, 2)
	assert.Equal(t, "a", popped[0].Member)
	assert.Equal(t, "b", popped[1].Member)

	assert.Equal(t, int64(1), ss.Len())
}