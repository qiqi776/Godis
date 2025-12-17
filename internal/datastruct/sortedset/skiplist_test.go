package sortedset

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestSkiplist_Insert(t *testing.T) {
	sl := makeskiplist()

	// 插入元素
	node1 := sl.insert("a", 1.0)
	node2 := sl.insert("b", 2.0)
	node3 := sl.insert("c", 3.0)

	// 验证插入后的顺序和元素
	assert.Equal(t, node1.Member, "a")
	assert.Equal(t, node2.Member, "b")
	assert.Equal(t, node3.Member, "c")
	assert.Equal(t, node1.Score, 1.0)
	assert.Equal(t, node2.Score, 2.0)
	assert.Equal(t, node3.Score, 3.0)

	// 验证跳表长度
	assert.Equal(t, sl.length, int64(3))
}

func TestSkiplist_RemoveNode(t *testing.T) {
	sl := makeskiplist()

	// 插入元素
	sl.insert("a", 1.0)
	sl.insert("b", 2.0)
	sl.insert("c", 3.0)

	// 删除元素
	removed := sl.isremove("b", 2.0)

	// 验证删除操作
	assert.True(t, removed)
	assert.Equal(t, sl.length, int64(2))

	// 验证剩下的元素
	node := sl.getByRank(0)
	assert.NotNil(t, node)
	assert.Equal(t, node.Member, "a")

	node = sl.getByRank(1)
	assert.NotNil(t, node)
	assert.Equal(t, node.Member, "c")
}

func TestSkiplist_RemoveRangeByRank(t *testing.T) {
	sl := makeskiplist()

	// 插入元素
	sl.insert("a", 1.0)
	sl.insert("b", 2.0)
	sl.insert("c", 3.0)

	// 删除指定排名范围内的元素
	removed := sl.RemoveRangeByRank(0, 1) 

	// 验证删除后的结果
	assert.Len(t, removed, 2) // 移除了前两个元素 a 和 b
	assert.Equal(t, removed[0].Member, "a")
	assert.Equal(t, removed[1].Member, "b")
	assert.Equal(t, sl.length, int64(1)) // 只剩下 "c"
}

func TestSkiplist_RemoveRange(t *testing.T) {
	sl := makeskiplist()

	// 插入元素
	sl.insert("a", 1.0)
	sl.insert("b", 2.0)
	sl.insert("c", 3.0)
	sl.insert("d", 4.0)

	// 删除指定范围内的元素
	removed := sl.RemoveRange(&ScoreBorder{Value: 1.5, Exclude: true}, &ScoreBorder{Value: 3.5, Exclude: true}, 0)

	// 验证删除后的结果
	assert.Len(t, removed, 2) // 移除了 b 和 c
	assert.Equal(t, removed[0].Member, "b")
	assert.Equal(t, removed[1].Member, "c")
	assert.Equal(t, sl.length, int64(2)) // 只剩下 "a" 和 "d"
}

func TestSkiplist_GetFirstInRange(t *testing.T) {
	sl := makeskiplist()

	// 插入元素
	sl.insert("a", 1.0)
	sl.insert("b", 2.0)
	sl.insert("c", 3.0)
	sl.insert("d", 4.0)

	// 获取范围内第一个元素
	node := sl.getFirstInRange(&ScoreBorder{Value: 1.5, Exclude: true}, &ScoreBorder{Value: 3.5, Exclude: true})
	assert.NotNil(t, node)
	assert.Equal(t, node.Member, "b")
}

func TestSkiplist_GetLastInRange(t *testing.T) {
	sl := makeskiplist()

	// 插入元素
	sl.insert("a", 1.0)
	sl.insert("b", 2.0)
	sl.insert("c", 3.0)
	sl.insert("d", 4.0)

	// 获取范围内最后一个元素
	node := sl.getLastInRange(&ScoreBorder{Value: 1.5, Exclude: true}, &ScoreBorder{Value: 3.5, Exclude: true})
	assert.NotNil(t, node)
	assert.Equal(t, node.Member, "c")
}

func TestSkiplist_GetRank(t *testing.T) {
	sl := makeskiplist()

	// 插入元素
	sl.insert("a", 1.0)
	sl.insert("b", 2.0)
	sl.insert("c", 3.0)
	sl.insert("d", 4.0)

	// 获取排名
	rank := sl.getRank("b", 2.0)
	assert.Equal(t, rank, int64(1)) // b 的排名是 1（从 0 开始）

	rank = sl.getRank("d", 4.0)
	assert.Equal(t, rank, int64(3)) // d 的排名是 3
}

func TestSkiplist_RandomLevel(t *testing.T) {
	// 测试 randomLevel 函数生成的随机层数
	levelCount := make(map[int]int)
	for i := 0; i < 10000; i++ {
		level := randomLevel()
		levelCount[level]++
	}

	// 检查层数分布
	for i := 1; i <= maxLevel; i++ {
		t.Logf("Level %d count: %d\n", i, levelCount[i])
	}
}
