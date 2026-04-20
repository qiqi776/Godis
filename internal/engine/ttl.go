package engine

import (
	"math"
	"time"
)

func (db *DB) Expire(key string, ttl time.Duration) bool {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.isExpired(key) {
		db.deleteKey(key)
		return false
	}
	if _, ok := db.data[key]; !ok {
		return false
	}
	if ttl <= 0 {
		db.deleteKey(key)
		return true
	}

	db.expireAt[key] = time.Now().Add(ttl)
	return true
}

func (db *DB) TTL(key string) int64 {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.isExpired(key) {
		db.deleteKey(key)
		return -2
	}
	if _, ok := db.data[key]; !ok {
		return -2
	}

	expireAt, ok := db.expireAt[key]
	if !ok {
		return -1
	}

	remaining := time.Until(expireAt)
	if remaining <= 0 {
		db.deleteKey(key)
		return -2
	}

	return int64(math.Ceil(remaining.Seconds()))
}

func (db *DB) Persist(key string) bool {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.isExpired(key) {
		db.deleteKey(key)
		return false
	}
	if _, ok := db.data[key]; !ok {
		return false
	}
	if _, ok := db.expireAt[key]; !ok {
		return false
	}

	delete(db.expireAt, key)
	db.touchKey(key)
	return true
}

func (db *DB) isExpired(key string) bool {
	expireAt, ok := db.expireAt[key]
	return ok && !expireAt.After(time.Now())
}

func (db *DB) deleteKey(key string) {
    _, hadData := db.data[key]
    _, hadExpire := db.expireAt[key]
    if !hadData && !hadExpire {
        return
    }

    delete(db.data, key)
    delete(db.expireAt, key)
    db.touchKey(key)
}