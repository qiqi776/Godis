package lsm

import (
	"bytes"
	"errors"
	"sort"
)

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
		err:   err,
	}
}

func newSliceIterator(entries []entry, err error) *Iterator {
    return &Iterator{
        entries: cloneEntries(entries),
        index:   -1,
		err: 	 err,	
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
        return bytes.Compare(it.entries[i].Key, key) >= 0
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
    return cloneBytes(it.entries[it.index].Key)
}

// 返回当前value
func (it *Iterator) Value() []byte {
    if !it.Valid() {
        return nil
    }
    return cloneBytes(it.entries[it.index].Value)
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
	if entries == nil {
		return nil
	}
    cloned := make([]entry, len(entries))
    for i := range entries {
        cloned[i] = entries[i].Clone()
    }
    return cloned
}

// 错误合并
func errorsJoin(err error, closeErr error) error {
	return errors.Join(err, closeErr)
}