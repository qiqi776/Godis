package engine

func getValue[T any](db *DB, key string, kind Kind) (T, bool, error) {
	var zero T

	if db.isExpired(key) {
		db.deleteKey(key)
		return zero, false, nil
	}

	entity, ok := db.data[key]
	if !ok {
		return zero, false, nil
	}
	if entity.Kind != kind {
		return zero, false, ErrWrongType
	}

	value, ok := entity.Value.(T)
	if !ok {
		return zero, false, ErrWrongType
	}
	return value, true, nil
}

func (db *DB) setValue(key string, kind Kind, value any) {
	db.data[key] = &Entity{
		Kind:  kind,
		Value: value,
	}
	db.touchKey(key)
}

func (db *DB) touchKey(key string) {
	db.nextRev++
	db.revisions[key] = db.nextRev
}

func (db *DB) Revision(key string) uint64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.isExpired(key) {
		db.deleteKey(key)
	}
	return db.revisions[key]
}