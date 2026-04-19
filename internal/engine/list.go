package engine

func (db *DB) LPush(key string, values ...[]byte) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	
	list, ok, err := db.getListValue(key)
	if err != nil {
		return 0, err
	}
	if !ok {
		list = nil
	}

	next := make([][]byte, 0, len(values)+len(list))
	for i := len(values) - 1; i >= 0; i-- {
		next = append(next, copyBytes(values[i]))
	}
	next = append(next, list...)

	db.data[key] = &Entity{
		Kind:  KindList,
		Value: next,
	}
	return int64(len(next)), nil
}

func (db *DB) RPush(key string, values ...[]byte) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	list, ok, err := db.getListValue(key)
	if err != nil {
		return 0, err
	}
	if !ok {
		list = nil
	}

	next := make([][]byte, 0, len(list)+len(values))
	next = append(next, list...)
	for _, value := range values {
		next = append(next, copyBytes(value))
	}

	db.data[key] = &Entity{
		Kind:  KindList,
		Value: next,
	}
	return int64(len(next)), nil
}

func (db *DB) LPop(key string) ([]byte, bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	list, ok, err := db.getListValue(key)
	if err != nil || !ok {
		return nil, false, err
	}

	value := copyBytes(list[0])
	if len(list) == 1 {
		db.deleteKey(key)
		return value, true, nil
	}

	next := copyByteSlices(list[1:])
	db.data[key] = &Entity{
		Kind:  KindList,
		Value: next,
	}
	return value, true, nil
}

func (db *DB) RPop(key string) ([]byte, bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	list, ok, err := db.getListValue(key)
	if err != nil || !ok {
		return nil, false, err
	}

	last := len(list) - 1
	value := copyBytes(list[last])
	if last == 0 {
		db.deleteKey(key)
		return value, true, nil
	}

	next := copyByteSlices(list[:last])
	db.data[key] = &Entity{
		Kind:  KindList,
		Value: next,
	}
	return value, true, nil
}

func (db *DB) LRange(key string, start, stop int64) ([][]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	list, ok, err := db.getListValue(key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return [][]byte{}, nil
	}

	from, to, ok := normalizeRange(len(list), start, stop)
	if !ok {
		return [][]byte{}, nil
	}
	return copyByteSlices(list[from : to+1]), nil
}

func (db *DB) getListValue(key string) ([][]byte, bool, error) {
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

	list, _ := entity.Value.([][]byte)
	return list, true, nil
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
