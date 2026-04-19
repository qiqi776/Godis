package set

import (
	"sort"

	"godis/internal/datastruct/dict"
)

type Set struct {
	data *dict.Dict
}

func New() *Set {
	return &Set{
		data: dict.New(),
	}
}

func (s *Set) Add(members ...string) int64 {
	var added int64
	for _, member := range members {
		added += s.data.Put(member, nil)
	}
	return added
}

func (s *Set) Remove(members ...string) int64 {
	var removed int64
	for _, member := range members {
		_, n := s.data.Remove(member)
		removed += n
	}
	return removed
}

func (s *Set) Has(member string) bool {
	_, ok := s.data.Get(member)
	return ok
}

func (s *Set) Len() int {
	return s.data.Len()
}

func (s *Set) Members() [][]byte {
	keys := s.data.Keys()
	sort.Strings(keys)

	out := make([][]byte, 0, len(keys))
	for _, member := range keys {
		out = append(out, []byte(member))
	}
	return out
}
