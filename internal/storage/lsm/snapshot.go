package lsm

import "bytes"

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
    iter := newIterator(s.entries)
    defer func() { _ = iter.Close() }()
    if !iter.Seek(key) || !bytes.Equal(iter.Key(), key) {
        return nil, false, nil
    }
    return iter.Value(), true, nil
}

// 范围扫描
func (s *Snapshot) NewIterator(options IterOptions) *Iterator {
    if s.closed {
        return newErrorIterator(ErrClosed)
    }
    entries := make([]entry, 0, len(s.entries))
    for _, item := range s.entries {
        if inBounds(item.key, options) {
            entries = append(entries, item)
        }
    }
    return newIterator(entries)
}

// 关闭快照
func (s *Snapshot) Close() error {
    s.closed = true
    s.entries = nil
    return nil
}