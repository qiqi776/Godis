package engine

import "godis/internal/datastruct/list"

func (db *DB) LPush(key string, values ...[]byte) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	lst, ok, err := getValue[*list.List](db, key, KindList)
	if err != nil {
		return 0, err
	}
	if !ok {
		lst = list.New()
		db.setValue(key, KindList, lst)
	}

	for _, value := range values {
		lst.PushFront(value)
	}
	return int64(lst.Len()), nil
}

func (db *DB) RPush(key string, values ...[]byte) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	lst, ok, err := getValue[*list.List](db, key, KindList)
	if err != nil {
		return 0, err
	}
	if !ok {
		lst = list.New()
		db.setValue(key, KindList, lst)
	}

	for _, value := range values {
		lst.PushBack(value)
	}
	return int64(lst.Len()), nil
}

func (db *DB) LPop(key string) ([]byte, bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	lst, ok, err := getValue[*list.List](db, key, KindList)
	if err != nil || !ok {
		return nil, false, err
	}

	value, ok := lst.PopFront()
	if !ok {
		return nil, false, nil
	}
	if lst.Len() == 0 {
		db.deleteKey(key)
	}
	return value, true, nil
}

func (db *DB) RPop(key string) ([]byte, bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	lst, ok, err := getValue[*list.List](db, key, KindList)
	if err != nil || !ok {
		return nil, false, err
	}

	value, ok := lst.PopBack()
	if !ok {
		return nil, false, nil
	}
	if lst.Len() == 0 {
		db.deleteKey(key)
	}
	return value, true, nil
}

func (db *DB) LRange(key string, start, stop int64) ([][]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	lst, ok, err := getValue[*list.List](db, key, KindList)
	if err != nil {
		return nil, err
	}
	if !ok {
		return [][]byte{}, nil
	}

	from, to, ok := normalizeRange(lst.Len(), start, stop)
	if !ok {
		return [][]byte{}, nil
	}
	return lst.Range(from, to), nil
}

func normalizeRange(size int, start, stop int64) (int, int, bool) {
	if size == 0 {
		return 0, 0, false
	}

	s := normalizeIndex(start, size)
	e := normalizeIndex(stop, size)

	if s < 0 {
		s = 0
	}
	if e < 0 {
		return 0, 0, false
	}
	if s >= size {
		return 0, 0, false
	}
	if e >= size {
		e = size - 1
	}
	if s > e {
		return 0, 0, false
	}
	return s, e, true
}

func normalizeIndex(index int64, size int) int {
	if index < 0 {
		return size + int(index)
	}
	return int(index)
}
