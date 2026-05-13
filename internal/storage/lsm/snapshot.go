package lsm

import (
	"bytes"
	"sort"

	"mini-kv/internal/storage/lsm/record"
)

type Snapshot struct {
    entries []entry
    closed  bool
}

func newSnapshot(entries []entry) *Snapshot {
    return &Snapshot{
		entries: cloneEntries(entries),
	}
}

// 使用迭代器查询
func (s *Snapshot) Get(key []byte) ([]byte, bool, error) {
    if s.closed {
        return nil, false, ErrClosed
    }
    pos := sort.Search(len(s.entries), func(i int) bool {
		return bytes.Compare(s.entries[i].Key, key) >= 0
	})
	if pos >= len(s.entries) || !bytes.Equal(s.entries[pos].Key, key) {
		return nil, false, nil
	}
    return record.CloneBytes(s.entries[pos].Value), true, nil
}

// 范围扫描
func (s *Snapshot) NewIterator(options IterOptions) *Iterator {
    if s.closed {
        return newErrorIterator(ErrClosed)
    }
	bounds := makeKeyBounds(options)
    entries := make([]entry, 0, len(s.entries))
    for _, item := range s.entries {
		if bounds.Contains(item.Key) {
			entries = append(entries, item.Clone())
		}
	}
    return newSliceIterator(entries, nil)
}

// 关闭快照
func (s *Snapshot) Close() error {
    s.closed = true
    s.entries = nil
    return nil
}