package memtable

import (
	"bytes"
	"math"

	"mini-kv/internal/storage/lsm/record"
)

const (
	maxHeight   = 20                    // 跳表最大层数，限制内存且保持对数级性能
	defaultSeed = 0x9e3779b97f4a7c15    // 默认随机种子，取自黄金分割率的64位表示
)

// Table 是基于跳表实现的有序内存表
// 内部使用伪随机高度生成，保证平均 O(log n) 的插入和查找
// Table 本身不加锁，并发安全由外部调用者（Engine）通过锁保证
type Table struct {
	head   *node  // 哨兵头节点，next 数组长度固定为 maxHeight
	height int    // 当前实际使用的最大层级（初始为1）
	rng    uint64 // 伪随机数发生器状态
	size   int64  // 近似内存占用量（字节），用于冻结决策
	count  int    // 当前存储的 entry 总数
}

type node struct {
	entry record.Entry
	next  []*node
}

func New() *Table {
	return NewWithSeed(defaultSeed)
}

func NewWithSeed(seed uint64) *Table {
	if seed == 0 {
		seed = defaultSeed
	}
	return &Table{
		head:   &node{next: make([]*node, maxHeight)}, // 哨兵节点预分配满层指针
		height: 1,
		rng:    seed,
	}
}

// Apply 批量插入条目，常用于 WAL 回放或恢复
func (t *Table) Apply(entries []record.Entry) error {
	for _, entry := range entries {
		t.Add(entry)
	}
	return nil
}

// Put 插入一个 Put 类型的条目
func (t *Table) Put(key, value []byte, seq uint64) {
	t.Add(record.NewPut(key, value, seq))
}

// Delete 插入一个 Delete 类型的条目（逻辑删除）
func (t *Table) Delete(key []byte, seq uint64) {
	t.Add(record.NewDelete(key, seq))
}

// Add 将一条 entry 插入跳表
// 如果存在 Key 与 Seq 完全相同的条目，则原地替换值，避免产生冗余节点
func (t *Table) Add(entry record.Entry) {
	entry = entry.Clone() // 深拷贝，保证内存隔离

	var update [maxHeight]*node           // 记录各层插入位置的前驱节点
	t.findPrev(entry, update[:])          // 查找前驱

	next := update[0].next[0] // 第 0 层待插入位置的下一个节点
	// 原地替换优化：相同 Key 且相同 Seq 的条目直接覆盖，避免节点膨胀
	if next != nil && record.Compare(next.entry, entry) == 0 {
		t.size += approximateSize(entry) - approximateSize(next.entry)
		next.entry = entry
		return
	}

	// 随机生成新节点的高度（1 ~ maxHeight，概率递减）
	height := t.randomHeight()
	// 若新节点高度超过当前表高，需将多出的层的 update 指向哨兵 head
	if height > t.height {
		for i := t.height; i < height; i++ {
			update[i] = t.head
		}
		t.height = height
	}

	insert := &node{
		entry: entry,
		next:  make([]*node, height),
	}
	// 在各层执行链表插入操作
	for i := 0; i < height; i++ {
		insert.next[i] = update[i].next[i]
		update[i].next[i] = insert
	}

	t.size += approximateSize(entry)
	t.count++
}

// Get 查找键对应的可见条目
// readSeq 为读取时的快照序列号，只有 Seq <= readSeq 的版本才可见
// 返回值: (entry, 是否存在)若存在则返回深拷贝的 entry
func (t *Table) Get(key []byte, readSeq uint64) (record.Entry, bool) {
	// 定位到目标 Key 版本链的最前端（Seq 最大的节点）
	cur := t.lowerBound(record.Entry{
		Key: key,
		Seq: math.MaxUint64, // 保证找到同 Key 的最大 Seq
	})
	// 遍历同一 Key 的连续版本链，找到第一个 Seq <= readSeq 的可见版本
	for cur != nil && bytes.Equal(cur.entry.Key, key) {
		if cur.entry.Seq <= readSeq {
			return cur.entry.Clone(), true
		}
		cur = cur.next[0]
	}
	return record.Entry{}, false
}

// findPrev 在跳表中查找 entry 在各层的插入前驱，结果存入 update 切片
// update[i] 表示第 i 层待插入位置的前一个节点
func (t *Table) findPrev(entry record.Entry, update []*node) {
	cur := t.head
	// 从最高层向下查找
	for level := t.height - 1; level >= 0; level-- {
		// 在该层尽量向右移动，直到下一个节点 >= entry
		for cur.next[level] != nil && record.Compare(cur.next[level].entry, entry) < 0 {
			cur = cur.next[level]
		}
		update[level] = cur
	}
}

// randomHeight 按照概率生成新节点的高度
// 每层递增的概率为 1/4，最高不超过 maxHeight
func (t *Table) randomHeight() int {
	height := 1
	for height < maxHeight && t.nextRand()&0x3 == 0 {
		height++
	}
	return height
}

// nextRand 是 64 位 xorshift 伪随机数生成器，快速且无外部依赖
func (t *Table) nextRand() uint64 {
	x := t.rng
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	t.rng = x
	return x
}

// approximateSize 估算一条 entry 带来的内存开销（字节）
// 用于追踪表大小，决定何时冻结并刷入磁盘
func approximateSize(entry record.Entry) int64 {
	return int64(len(entry.Key) + len(entry.Value) + 24)
}

// lowerBound 返回第一个 >= 给定 entry 的节点（基于 record.Compare 排序规则）
// 若没有则返回 nil
func (t *Table) lowerBound(entry record.Entry) *node {
	cur := t.head
	for level := t.height - 1; level >= 0; level-- {
		for cur.next[level] != nil && record.Compare(cur.next[level].entry, entry) < 0 {
			cur = cur.next[level]
		}
	}
	return cur.next[0]
}

// Freeze 将当前内存表冻结为只读的 Immutable，用于后续刷盘
// 冻结后本表不应再接受写入（由外部保证）
func (t *Table) Freeze() *Immutable {
	return &Immutable{table: t}
}

// Entries 按顺序返回表中所有 entry 的深拷贝切片
// 遍历跳表第 0 层即可获得全量有序数据
func (t *Table) Entries() []record.Entry {
	entries := make([]record.Entry, 0, t.count)
	for current := t.head.next[0]; current != nil; current = current.next[0] {
		entries = append(entries, current.entry.Clone())
	}
	return entries
}

// Len 返回表中当前 entry 数量
func (t *Table) Len() int {
	return t.count
}

// ApproximateSize 返回估算的内存占用量（字节）
func (t *Table) ApproximateSize() int64 {
	return t.size
}

// Immutable 是不可变内存表的只读视图，等待刷入 SSTable
// 所有方法均委托给内部的 *Table，但语义上禁止写入
type Immutable struct {
	table *Table
}

// Get 在不可变表中查找键
func (i *Immutable) Get(key []byte, readSeq uint64) (record.Entry, bool) {
	return i.table.Get(key, readSeq)
}

// NewIterator 创建不可变表的迭代器，支持范围扫描和快照隔离
func (i *Immutable) NewIterator(readSeq uint64, bounds record.KeyBounds) *Iterator {
	return i.table.NewIterator(readSeq, bounds)
}

// ApproximateSize 返回不可变表的内存估算值
func (i *Immutable) ApproximateSize() int64 {
	return i.table.ApproximateSize()
}

// Entries 返回不可变表中所有 entry 的深拷贝切片
func (i *Immutable) Entries() []record.Entry {
	return i.table.Entries()
}

// Iterator 是 MemTable 专用的内部迭代器，直接基于跳表节点遍历
// 会动态过滤 readSeq（快照隔离）和 bounds（键范围），每个 Key 仅返回最新可见版本
type Iterator struct {
	table   *Table
	readSeq uint64          // 读取快照序列号，跳过更晚的写入
	bounds  record.KeyBounds // 键范围约束，左闭右开
	cur     *node            // 当前指向的节点
}

// NewIterator 创建一个新的 MemTable 迭代器
func (t *Table) NewIterator(readSeq uint64, bounds record.KeyBounds) *Iterator {
	return &Iterator{
		table:   t,
		readSeq: readSeq,
		bounds:  bounds,
	}
}

// First 将迭代器定位到范围内第一个可见条目
func (it *Iterator) First() bool {
	seek := it.bounds.Lower
	it.cur = it.table.lowerBound(record.Entry{Key: seek, Seq: math.MaxUint64})
	return it.advanceVisible()
}

// Seek 将迭代器定位到 >= key 的第一个可见条目
// 若 key 小于下界，会自动提升至下界
func (it *Iterator) Seek(key []byte) bool {
	seek := it.bounds.NormalizeSeek(key)
	it.cur = it.table.lowerBound(record.Entry{Key: seek, Seq: math.MaxUint64})
	return it.advanceVisible()
}

// Next 移动到下一个不同 Key 的第一个可见版本
func (it *Iterator) Next() bool {
	if it.cur == nil {
		return false
	}
	key := it.cur.entry.Key
	// 跳过当前 Key 的剩余版本（它们不可能更优）
	for it.cur != nil && bytes.Equal(it.cur.entry.Key, key) {
		it.cur = it.cur.next[0]
	}
	return it.advanceVisible()
}

// Valid 判断当前迭代器是否指向有效条目
func (it *Iterator) Valid() bool {
	return it.cur != nil && it.bounds.Contains(it.cur.entry.Key)
}

// Entry 返回当前条目的深拷贝
// 必须在 Valid() == true 时调用
func (it *Iterator) Entry() record.Entry {
	if !it.Valid() {
		return record.Entry{}
	}
	return it.cur.entry.Clone()
}

// Err 返回迭代过程中的错误当前实现始终无错误
func (it *Iterator) Err() error {
	return nil
}

// Close 关闭迭代器，释放当前节点引用
func (it *Iterator) Close() error {
	it.cur = nil
	return nil
}

// advanceVisible 从当前位置开始，推进到下一个满足 bounds 且可见的条目
// 返回 true 表示成功定位，false 表示迭代结束
func (it *Iterator) advanceVisible() bool {
	for it.cur != nil {
		// 越界检查：超出上界则终止
		if !it.bounds.Contains(it.cur.entry.Key) {
			it.cur = nil
			return false
		}

		key := it.cur.entry.Key
		// 在同 Key 版本链中寻找第一个 Seq <= readSeq 的版本
		for it.cur != nil && bytes.Equal(it.cur.entry.Key, key) {
			if it.cur.entry.Seq <= it.readSeq {
				return true // 找到可见版本
			}
			it.cur = it.cur.next[0] // 该版本不可见，尝试下一个更旧的版本
		}
		// 当前 Key 的所有版本都不可见，继续处理下一个 Key（外层循环）
	}
	return false
}