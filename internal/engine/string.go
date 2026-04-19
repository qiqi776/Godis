package engine

func (db *DB) Get(key string) ([]byte, bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.isExpired(key) {
		db.deleteKey(key)
		return nil, false, nil
	}

	entity, ok := db.data[key]
	if !ok {
		return nil, false, nil
	}
	if entity.Kind != KindString {
		return nil, false, ErrWrongType
	}

	val, _ := entity.Value.([]byte)
	return copyBytes(val), true, nil
}

func (db *DB) Set(key string, val []byte) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.data[key] = &Entity{
		Kind:  KindString,
		Value: copyBytes(val),
	}
	delete(db.expireAt, key)
}
