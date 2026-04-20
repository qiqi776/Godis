package engine

import (
	"sync"
	"time"
)

type Engine struct {
	dbs []*DB
}

type DB struct {
	mu        sync.RWMutex
	data      map[string]*Entity
	expireAt  map[string]time.Time
	revisions map[string]uint64
	nextRev   uint64
}

func New(databases int) *Engine {
	if databases <= 0 {
		databases = 16
	}

	dbs := make([]*DB, 0, databases)
	for i := 0; i < databases; i++ {
		dbs = append(dbs, &DB{
			data:      make(map[string]*Entity),
			expireAt:  make(map[string]time.Time),
			revisions: make(map[string]uint64),
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
