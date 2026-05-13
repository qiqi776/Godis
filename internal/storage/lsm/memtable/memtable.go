package memtable

import (
	"bytes"
	"math"

	"mini-kv/internal/storage/lsm/record"
)
const (
	maxHeight   = 20
	defaultSeed = 0x9e3779b97f4a7c15
)

type Table struct {
	head   *node
	height int
	rng    uint64
	size   int64
	count  int
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
		head:   &node{next: make([]*node, maxHeight)},
		height: 1,
		rng:    seed,
	}
}

func (t *Table) Apply(entries []record.Entry) error {
	for _, entry := range entries {
		t.Add(entry)
	}
	return nil
}

func (t *Table) Put(key, value []byte, seq uint64) {
	t.Add(record.NewPut(key, value, seq))
}

func (t *Table) Delete(key []byte, seq uint64) {
	t.Add(record.NewDelete(key, seq))
}

// 插入条目
func (t *Table) Add(entry record.Entry) {
	entry = entry.Clone()
	var update [maxHeight]*node
	t.findPrev(entry, update[:])
	next := update[0].next[0]
	if next != nil && record.Compare(next.entry, entry) == 0 {
		t.size += approximateSize(entry) - approximateSize(next.entry)
		next.entry = entry
		return
	}
	
	height := t.randomHeight()
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
	for i := 0; i < height; i++ {
		insert.next[i] = update[i].next[i]
		update[i].next[i] = insert
	}
	t.size += approximateSize(entry)
	t.count++
}

// 查找条目
func (t *Table) Get(key []byte, readSeq uint64) (record.Entry, bool) {
	cur := t.lowerBound(record.Entry{
		Key: key,
		Seq: math.MaxUint64,
	})
	for cur != nil && bytes.Equal(cur.entry.Key, key) {
		if cur.entry.Seq <= readSeq {
			return cur.entry.Clone(), true
		}
		cur = cur.next[0]
	}
	return record.Entry{}, false
}

// 查找前驱节点
func (t *Table) findPrev(entry record.Entry, update []*node) {
	cur := t.head
	for level := t.height - 1; level >= 0; level-- {
		for cur.next[level] != nil && record.Compare(cur.next[level].entry, entry) < 0 {
			cur = cur.next[level]
		}
		update[level] = cur
	}
}

// 生成随机高度
func (t *Table) randomHeight() int {
	height := 1
	for height < maxHeight && t.nextRand()&0x3 == 0 {
		height++
	}
	return height
}

func (t *Table) nextRand() uint64 {
	x := t.rng
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	t.rng = x
	return x
}

func approximateSize(entry record.Entry) int64 {
	return int64(len(entry.Key) + len(entry.Value) + 24)
}

func (t *Table) lowerBound(entry record.Entry) *node {
	cur := t.head
	for level := t.height - 1; level >= 0; level-- {
		for cur.next[level] != nil && record.Compare(cur.next[level].entry, entry) < 0 {
			cur = cur.next[level]
		}
	}
	return cur.next[0]
}

func (t *Table) Freeze() *Immutable {
	return &Immutable{table: t}
}

func (t *Table) Entries() []record.Entry {
	entries := make([]record.Entry, 0, t.count)
	for current := t.head.next[0]; current != nil; current = current.next[0] {
		entries = append(entries, current.entry.Clone())
	}
	return entries
}

func (t *Table) Len() int {
	return t.count
}

func (t *Table) ApproximateSize() int64 {
	return t.size
}

type Immutable struct {
	table *Table
}

func (i *Immutable) Get(key []byte, readSeq uint64) (record.Entry, bool) {
	return i.table.Get(key, readSeq)
}

func (i *Immutable) NewIterator(readSeq uint64, bounds record.KeyBounds) *Iterator {
	return i.table.NewIterator(readSeq, bounds)
}

func (i *Immutable) ApproximateSize() int64 {
	return i.table.ApproximateSize()
}

func (i *Immutable) Entries() []record.Entry {
	return i.table.Entries()
}

type Iterator struct {
	table   *Table
	readSeq uint64
	bounds  record.KeyBounds
	cur     *node
}

func (t *Table) NewIterator(readSeq uint64, bounds record.KeyBounds) *Iterator {
	return &Iterator{
		table:   t,
		readSeq: readSeq,
		bounds:  bounds,
	}
}

// 移动到首个满足条件的可见条目
func (it *Iterator) First() bool {
	seek := it.bounds.Lower
	it.cur = it.table.lowerBound(record.Entry{Key: seek, Seq: math.MaxUint64})
	return it.advanceVisible()
}

func (it *Iterator) Seek(key []byte) bool {
	seek := it.bounds.NormalizeSeek(key)
	it.cur = it.table.lowerBound(record.Entry{Key: seek, Seq: math.MaxUint64})
	return it.advanceVisible()
}

func (it *Iterator) Next() bool {
	if it.cur == nil {
		return false
	}
	key := it.cur.entry.Key
	for it.cur != nil && bytes.Equal(it.cur.entry.Key, key) {
		it.cur = it.cur.next[0]
	}
	return it.advanceVisible()
}

func (it *Iterator) Valid() bool {
	return it.cur != nil && it.bounds.Contains(it.cur.entry.Key)
}

func (it *Iterator) Entry() record.Entry {
	if !it.Valid() {
		return record.Entry{}
	}
	return it.cur.entry.Clone()
}

func (it *Iterator) Err() error {
	return nil
}

func (it *Iterator) Close() error {
	it.cur = nil
	return nil
}

func (it *Iterator) advanceVisible() bool {
	for it.cur != nil {
		if !it.bounds.Contains(it.cur.entry.Key) {
			it.cur = nil
			return false
		}
		key := it.cur.entry.Key
		for it.cur != nil && bytes.Equal(it.cur.entry.Key, key) {
			if it.cur.entry.Seq <= it.readSeq {
				return true
			}
			it.cur = it.cur.next[0]
		}
	}
	return false
}