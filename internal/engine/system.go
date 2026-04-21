package engine

func (e *Engine) DBCount() int {
	return len(e.dbs)
}

func (db *DB) DBSize() int64 {
	db.mu.Lock()
	defer db.mu.Unlock()

	var count int64
	for key := range db.data {
		if db.isExpired(key) {
			db.deleteKey(key)
			continue
		}
		count++
	}
	return count
}

func (db *DB) Type(key string) string {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.isExpired(key) {
		db.deleteKey(key)
		return "none"
	}

	entity, ok := db.data[key]
	if !ok {
		return "none"
	}
	return kindName(entity.Kind)
}

func kindName(kind Kind) string {
	switch kind {
	case KindString:
		return "string"
	case KindList:
		return "list"
	case KindHash:
		return "hash"
	case KindSet:
		return "set"
	case KindZSet:
		return "zset"
	case KindBitmap:
		return "bitmap"
	default:
		return "none"
	}
}