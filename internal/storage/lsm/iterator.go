package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

	"mini-kv/internal/storage/lsm/record"
)

// Iterator 是基于有序条目切片的公共迭代器，用于对外暴露 Key/Value 接口
// 它持有独立的数据副本，因此不受引擎后续写入影响
type Iterator struct {
	entries []entry // 有序条目切片（按键升序）
	index   int     // 当前游标，-1 表示未定位或越界
	err     error   // 持久性错误（由构造时传入，若不为 nil 则迭代器始终无效）
	closed  bool    // 是否已关闭
}

// newErrorIterator 创建一个携带错误的迭代器，其 Valid() 始终返回 false
func newErrorIterator(err error) *Iterator {
	return &Iterator{
		index: -1,
		err:   err,
	}
}

// newSliceIterator 基于给定的条目切片和可选的错误构造迭代器
// 切片会被深拷贝，以保证迭代器数据独立
func newSliceIterator(entries []entry, err error) *Iterator {
	return &Iterator{
		entries: cloneEntries(entries),
		index:   -1,
		err:     err,
	}
}

// First 移动到第一个条目若已关闭或切片为空则返回 false
func (it *Iterator) First() bool {
	if it.closed || len(it.entries) == 0 {
		it.index = -1
		return false
	}
	it.index = 0
	return true
}

// Seek 二分查找并定位到第一个 Key >= 指定键的条目
func (it *Iterator) Seek(key []byte) bool {
	if it.closed || len(it.entries) == 0 {
		it.index = -1
		return false
	}
	pos := sort.Search(len(it.entries), func(i int) bool {
		return bytes.Compare(it.entries[i].Key, key) >= 0
	})
	if pos >= len(it.entries) {
		it.index = -1
		return false
	}
	it.index = pos
	return true
}

// Next 移动到下一个条目若当前未定位（index < 0）则等价于 First
func (it *Iterator) Next() bool {
	if it.closed {
		it.index = -1
		return false
	}
	if it.index < 0 {
		return it.First()
	}
	it.index++
	if it.index >= len(it.entries) {
		it.index = -1
		return false
	}
	return true
}

// Valid 返回当前游标是否指向有效条目
func (it *Iterator) Valid() bool {
	return !it.closed && it.index >= 0 && it.index < len(it.entries)
}

// Key 返回当前条目的键的深拷贝；若无效则返回 nil
func (it *Iterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return record.CloneBytes(it.entries[it.index].Key)
}

// Value 返回当前条目的值的深拷贝；若无效则返回 nil
func (it *Iterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return record.CloneBytes(it.entries[it.index].Value)
}

// Error 返回迭代过程中发生的持久错误
func (it *Iterator) Error() error {
	return it.err
}

// Close 关闭迭代器，释放内部数据引用
func (it *Iterator) Close() error {
	it.closed = true
	it.index = -1
	it.entries = nil
	return nil
}

// cloneEntries 对条目切片进行深拷贝，保证数据独立
func cloneEntries(entries []entry) []entry {
	if entries == nil {
		return nil
	}
	cloned := make([]entry, len(entries))
	for i := range entries {
		cloned[i] = entries[i].Clone()
	}
	return cloned
}

// collectVisibleEntries 从给定的内存视图（活跃 MemTable 及所有不可变 MemTable）中，
// 收集所有在 readSeq 下可见、且落在 bounds 范围内的条目，去重并只保留每个键的最新可见版本
// 返回按 Key 升序排列的条目切片
func collectVisibleEntries(view *memView, readSeq uint64, bounds keyBounds) ([]entry, error) {
	if view == nil {
		return nil, nil
	}

	// latest 以键的字符串形式作为 key，记录当前看到的最新条目（Seq 最大）
	latest := make(map[string]entry)

	// collect 闭包遍历一个内部迭代器，更新 latest 映射
	collect := func(iter internalIterator) error {
		if iter == nil {
			return nil
		}
		for ok := iter.First(); ok; ok = iter.Next() {
			item := iter.Entry()
			// 跳过不可见或超出范围的条目
			if item.Seq > readSeq || !bounds.Contains(item.Key) {
				continue
			}
			key := string(item.Key)
			current, exists := latest[key]
			// 保留 Seq 更大的版本（即更新的版本）
			if !exists || item.Seq > current.Seq {
				latest[key] = item.Clone()
			}
		}
		err := iter.Err()
		if closeErr := iter.Close(); closeErr != nil {
			err = errorsJoin(err, closeErr)
		}
		return err
	}

	// 先收集活跃 MemTable
	if view.mutable != nil {
		if err := collect(view.mutable.NewIterator(readSeq, bounds)); err != nil {
			return nil, fmt.Errorf("collect mutable memtable: %w", err)
		}
	}
	// 再从最新到最旧收集不可变 MemTable
	for i := len(view.immutable) - 1; i >= 0; i-- {
		if view.immutable[i] == nil {
			continue
		}
		if err := collect(view.immutable[i].NewIterator(readSeq, bounds)); err != nil {
			return nil, fmt.Errorf("collect immutable memtable: %w", err)
		}
	}

	// 从 latest 映射生成最终条目切片
	entries := make([]entry, 0, len(latest))
	for _, item := range latest {
		// 若最终版本是 Delete，则该键应被视为不存在，丢弃
		if item.Kind == record.KindDelete {
			continue
		}
		if item.Kind != record.KindPut {
			return nil, fmt.Errorf("%w: unknown entry kind", ErrCorrupt)
		}
		entries = append(entries, item.Clone())
	}
	// 按键升序排序，保证输出有序
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Key, entries[j].Key) < 0
	})
	return entries, nil
}

// mergeVisibleEntries 合并多组已经预处理过的条目切片，去重并只保留每个键的最新可见版本
// 结果按键升序排序，可用于组合内存层和 SSTable 层的条目
func mergeVisibleEntries(groups ...[]entry) ([]entry, error) {
	latest := make(map[string]entry)
	for _, entries := range groups {
		for _, item := range entries {
			key := string(item.Key)
			current, ok := latest[key]
			if !ok || item.Seq > current.Seq {
				latest[key] = item.Clone()
			}
		}
	}
	merged := make([]entry, 0, len(latest))
	for _, item := range latest {
		if item.Kind == record.KindDelete {
			continue
		}
		if item.Kind != record.KindPut {
			return nil, fmt.Errorf("%w: unknown entry kind", ErrCorrupt)
		}
		merged = append(merged, item.Clone())
	}
	sort.Slice(merged, func(i, j int) bool {
		return bytes.Compare(merged[i].Key, merged[j].Key) < 0
	})
	return merged, nil
}

// errorsJoin 将两个错误合并为一个，兼容 errors.Join
func errorsJoin(err error, closeErr error) error {
	return errors.Join(err, closeErr)
}