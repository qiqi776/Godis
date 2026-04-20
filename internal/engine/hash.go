package engine

import "godis/internal/datastruct/hash"

func (db *DB) HSet(key, field string, value []byte) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	h, ok, err := getValue[*hash.Hash](db, key, KindHash)
	if err != nil {
		return 0, err
	}
	if !ok {
		h = hash.New()
		db.setValue(key, KindHash, h)
	} else {
		db.touchKey(key)
	}
	return h.Set(field, value), nil
}

func (db *DB) HGet(key, field string) ([]byte, bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	h, ok, err := getValue[*hash.Hash](db, key, KindHash)
	if err != nil || !ok {
		return nil, false, err
	}

	value, ok := h.Get(field)
	return value, ok, nil
}

func (db *DB) HDel(key string, fields ...string) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	h, ok, err := getValue[*hash.Hash](db, key, KindHash)
	if err != nil || !ok {
		return 0, err
	}

	deleted := h.Del(fields...)
	if h.Len() == 0 {
		db.deleteKey(key)
	} else {
		db.touchKey(key)
	}
	return deleted, nil
}

func (db *DB) HGetAll(key string) ([][]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	h, ok, err := getValue[*hash.Hash](db, key, KindHash)
	if err != nil {
		return nil, err
	}
	if !ok {
		return [][]byte{}, nil
	}

	return h.GetAll(), nil
}
