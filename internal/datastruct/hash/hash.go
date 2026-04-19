package hash

import (
	"sort"

	"godis/internal/datastruct/dict"
)

type Hash struct {
	data *dict.Dict
}

func New() *Hash {
	return &Hash{
		data: dict.New(),
	}
}

func (h *Hash) Set(field string, value []byte) int64 {
	return h.data.Put(field, copyBytes(value))
}

func (h *Hash) Get(field string) ([]byte, bool) {
	value, ok := h.data.Get(field)
	if !ok {
		return nil, false
	}
	raw, _ := value.([]byte)
	return copyBytes(raw), true
}

func (h *Hash) Del(fields ...string) int64 {
	var deleted int64
	for _, field := range fields {
		_, n := h.data.Remove(field)
		deleted += n
	}
	return deleted
}

func (h *Hash) Len() int {
	return h.data.Len()
}

func (h *Hash) GetAll() [][]byte {
	keys := h.data.Keys()
	sort.Strings(keys)

	out := make([][]byte, 0, len(keys)*2)
	for _, field := range keys {
		out = append(out, []byte(field))
		value, _ := h.data.Get(field)
		raw, _ := value.([]byte)
		out = append(out, copyBytes(raw))
	}
	return out
}

func copyBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	return append([]byte(nil), src...)
}
