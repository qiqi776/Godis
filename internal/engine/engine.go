package engine

import (
	"math"
	"sync"
	"time"
)

type Engine struct {
	dbs []*DB
}

type DB struct {
	mu       sync.RWMutex
	data     map[string][]byte
	expireAt map[string]time.Time
}

func New(databases int) *Engine {
	if databases <= 0 {
		databases = 16
	}

	dbs := make([]*DB, 0, databases)
	for i := 0; i < databases; i++ {
		dbs = append(dbs, &DB{
			data:     make(map[string][]byte),
			expireAt: make(map[string]time.Time),
		})
	}

	return &Engine{dbs: dbs}
}

func (e *Engine) Close() {}

func (e *Engine) DB(index int) *DB {
	if index < 0 || index >= len(e.dbs) {
		return nil
	}
	return e.dbs[index]
}

func (db *DB) Get(key string) ([]byte, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.isExpired(key) {
		db.deleteKey(key)
		return nil, false
	}

	val, ok := db.data[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), val...), true
}

func (db *DB) Set(key string, val []byte) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.data[key] = append([]byte(nil), val...)
	delete(db.expireAt, key)
}

func (db *DB) Del(keys ...string) int64 {
	db.mu.Lock()
	defer db.mu.Unlock()

	var deleted int64
	for _, key := range keys {
		if db.isExpired(key) {
			db.deleteKey(key)
			continue
		}
		if _, ok := db.data[key]; ok {
			db.deleteKey(key)
			deleted++
		}
	}
	return deleted
}

func (db *DB) Exists(keys ...string) int64 {
	db.mu.Lock()
	defer db.mu.Unlock()

	var count int64
	for _, key := range keys {
		if db.isExpired(key) {
			db.deleteKey(key)
			continue
		}
		if _, ok := db.data[key]; ok {
			count++
		}
	}
	return count
}

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
	return true
}

func (db *DB) isExpired(key string) bool {
	expireAt, ok := db.expireAt[key]
	return ok && !expireAt.After(time.Now())
}

func (db *DB) deleteKey(key string) {
	delete(db.data, key)
	delete(db.expireAt, key)
}
