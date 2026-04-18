package engine

import (
	"sync"

)

type Engine struct {
	dbs []*DB
}

type DB struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewEngine(databases int) *Engine {
	if databases <= 0 {
		databases = 16
	}
	dbs := make([]*DB, 0, databases)
	for i := 0; i < databases; i++ {
		dbs = append(dbs, &DB{
			data: make(map[string][]byte),
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
	db.mu.RLock()
	defer db.mu.RUnlock()

	val, ok := db.data[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), val...), true
}

func (db *DB) Set(key string, val []byte) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.data[key] = append(db.data[key], val...)
}

func (db *DB) Del(keys ...string) int64 {
    db.mu.Lock()
    defer db.mu.Unlock()

    var deleted int64
    for _, key := range keys {
        if _, ok := db.data[key]; ok {
            delete(db.data, key)
			deleted++
        }
    }
    return deleted
}

func (db *DB) Exists(keys ...string) int64 {
    db.mu.RLock()
    defer db.mu.RUnlock()

    var count int64
    for _, key := range keys {
        if _, ok := db.data[key]; ok {
            count++
        }
    }
    return count
}