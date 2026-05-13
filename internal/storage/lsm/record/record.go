package record

import "bytes"

type Kind uint8

const (
	KindUnknown Kind = iota
	KindPut
	KindDelete
)

// 条目
type Entry struct {
	Key   []byte
	Value []byte
	Seq   uint64
	Kind  Kind
}

// 批量操作
type Batch struct {
	SeqStart uint64
	Entries  []Entry
}

// Key 边界
type KeyBounds struct {
	Lower []byte
	Upper []byte
}

func NewPut(key, value []byte, seq uint64) Entry {
	return Entry{
		Key:   CloneBytes(key),
		Value: CloneBytes(value),
		Seq:   seq,
		Kind:  KindPut,
	}
}

func NewDelete(key []byte, seq uint64) Entry {
	return Entry{
		Key:  CloneBytes(key),
		Seq:  seq,
		Kind: KindDelete,
	}
}

func (e Entry) Clone() Entry {
	return Entry{
		Key:   CloneBytes(e.Key),
		Value: CloneBytes(e.Value),
		Seq:   e.Seq,
		Kind:  e.Kind,
	}
}

func (b Batch) Clone() Batch {
	entries := make([]Entry, len(b.Entries))
	for i := range b.Entries {
		entries[i] = b.Entries[i].Clone()
	}
	return Batch{
		SeqStart: b.SeqStart,
		Entries:  entries,
	}
}

func (b KeyBounds) Clone() KeyBounds {
	return KeyBounds{
		Lower: CloneBytes(b.Lower),
		Upper: CloneBytes(b.Upper),
	}
}

func (b KeyBounds) Contains(key []byte) bool {
	if len(b.Lower) > 0 && bytes.Compare(key, b.Lower) < 0 {
		return false
	}
	if len(b.Upper) > 0 && bytes.Compare(key, b.Upper) >= 0 {
		return false
	}
	return true
}

func (b KeyBounds) NormalizeSeek(key []byte) []byte {
	if len(b.Lower) > 0 && bytes.Compare(key, b.Lower) < 0 {
		return b.Lower
	}
	return key
}

// 排序规则：先按 Key 升序，再按 Seq 降序
func Compare(a, b Entry) int {
	if cmp := bytes.Compare(a.Key, b.Key); cmp != 0 {
		return cmp
	}
	switch {
	case a.Seq > b.Seq:
		return -1
	case a.Seq < b.Seq:
		return 1
	default:
		return 0
	}
}

func SameKey(a, b []byte) bool {
	return bytes.Equal(a, b)
}

func CloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	cloned := make([]byte, len(value))
	copy(cloned, value)
	return cloned
}
