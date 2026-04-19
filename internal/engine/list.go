package engine

import listds "godis/internal/datastruct/list"

func (db *DB) LPush(key string, values ...[]byte) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	l, ok, err := db.getListValue(key)
	if err != nil {
		return 0, err
	}
	if !ok {
		l = listds.New()
		db.data[key] = &Entity{
			Kind:  KindList,
			Value: l,
		}
	}

	for _, value := range values {
		l.PushFront(value)
	}
	return int64(l.Len()), nil
}

func (db *DB) RPush(key string, values ...[]byte) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	l, ok, err := db.getListValue(key)
	if err != nil {
		return 0, err
	}
	if !ok {
		l = listds.New()
		db.data[key] = &Entity{
			Kind:  KindList,
			Value: l,
		}
	}

	for _, value := range values {
		l.PushBack(value)
	}
	return int64(l.Len()), nil
}

func (db *DB) LPop(key string) ([]byte, bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	l, ok, err := db.getListValue(key)
	if err != nil || !ok {
		return nil, false, err
	}

	value, ok := l.PopFront()
	if !ok {
		return nil, false, nil
	}
	if l.Len() == 0 {
		db.deleteKey(key)
	}
	return value, true, nil
}

func (db *DB) RPop(key string) ([]byte, bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	l, ok, err := db.getListValue(key)
	if err != nil || !ok {
		return nil, false, err
	}

	value, ok := l.PopBack()
	if !ok {
		return nil, false, nil
	}
	if l.Len() == 0 {
		db.deleteKey(key)
	}
	return value, true, nil
}

func (db *DB) LRange(key string, start, stop int64) ([][]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	l, ok, err := db.getListValue(key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return [][]byte{}, nil
	}

	from, to, ok := normalizeRange(l.Len(), start, stop)
	if !ok {
		return [][]byte{}, nil
	}
	return l.Range(from, to), nil
}

func (db *DB) getListValue(key string) (*listds.List, bool, error) {
	if db.isExpired(key) {
		db.deleteKey(key)
		return nil, false, nil
	}

	entity, ok := db.data[key]
	if !ok {
		return nil, false, nil
	}
	if entity.Kind != KindList {
		return nil, false, ErrWrongType
	}

	l, _ := entity.Value.(*listds.List)
	return l, true, nil
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
