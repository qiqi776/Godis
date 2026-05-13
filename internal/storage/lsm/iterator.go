package lsm

import (
    "bytes"
    "sort"
)

// 内部条目
type entry struct {
    key   []byte
    value []byte
}

// 迭代器
type Iterator struct {
    entries []entry
    index   int
    err     error
    closed  bool
}

func newErrorIterator(err error) *Iterator {
    return &Iterator{
		index: -1,
		err: err,
	}
}

func newIterator(entries []entry) *Iterator {
    return &Iterator{
        entries: cloneEntries(entries),
        index:   -1,
    }
}

// 移动到第一个条目
func (it *Iterator) First() bool {
    if it.closed || len(it.entries) == 0 {
        it.index = -1
        return false
    }
    it.index = 0
    return true
}

// 使用二分搜索定位第一个 >= key 的条目
func (it *Iterator) Seek(key []byte) bool {
    if it.closed || len(it.entries) == 0 {
        it.index = -1
        return false
    }
    pos := sort.Search(len(it.entries), func(i int) bool {
        return bytes.Compare(it.entries[i].key, key) >= 0
    })
    if pos >= len(it.entries) {
        it.index = -1
        return false
    }
    it.index = pos
    return true
}

// 将游标向后移动一位
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

// 迭代器状态
func (it *Iterator) Valid() bool {
    return !it.closed && it.index >= 0 && it.index < len(it.entries)
}

// 返回当前 key
func (it *Iterator) Key() []byte {
    if !it.Valid() {
        return nil
    }
    return cloneBytes(it.entries[it.index].key)
}

// 返回当前value
func (it *Iterator) Value() []byte {
    if !it.Valid() {
        return nil
    }
    return cloneBytes(it.entries[it.index].value)
}

func (it *Iterator) Error() error {
    return it.err
}

func (it *Iterator) Close() error {
    it.closed = true
    it.entries = nil
    it.index = -1
    return nil
}

// 深拷贝辅助函数
func cloneEntries(entries []entry) []entry {
    out := make([]entry, len(entries))
    for i := range entries {
        out[i] = entry{
            key:   cloneBytes(entries[i].key),
            value: cloneBytes(entries[i].value),
        }
    }
    return out
}