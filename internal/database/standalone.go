package database

import (
	"godis/internal/aof"
	"sync"
	"sync/atomic"
)

type StandaloneDB struct {
	data    map[string]string
	mu      sync.RWMutex
	aof    *aof.Aof
	Stats *Stats
}

func NewStandalone() *StandaloneDB {
	return &StandaloneDB{
		data:  make(map[string]string),
		aof:   nil,
		Stats: NewStats(),
	}
}

// 只负责存数据，不再解析 []protocol.Value
func (db *StandaloneDB) Set(key string, value []byte) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data[key] = string(value)
}

func (db *StandaloneDB) Get(key string) ([]byte, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	val, ok := db.data[key]
	
	if !ok {
		atomic.AddInt64(&db.Stats.KeyspaceMisses, 1)
		return nil, false
	}

	atomic.AddInt64(&db.Stats.KeyspaceHits, 1)
	return []byte(val), true
}

// 返回统计信息
func (d *StandaloneDB) GetStats() *Stats {
	return d.Stats
}

// 返回键数量
func (d *StandaloneDB) KeyCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.data)
}

// 注入AOF引擎
func (d *StandaloneDB) SetAof(a *aof.Aof) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.aof = a
}

// 获取AOF引擎
func (d *StandaloneDB) GetAof() *aof.Aof {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.aof
}