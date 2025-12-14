package database

import (
	"godis/internal/aof"
	"sync"
	"sync/atomic"
	"time"
)

type ExpirationVal struct {
	Val  []byte
	ExpiresAt time.Time
}

type StandaloneDB struct {
	data    map[string]ExpirationVal
	mu      sync.RWMutex
	aof    *aof.Aof
	Stats  *Stats
}

func NewStandalone() *StandaloneDB {
	return &StandaloneDB{
		data:  make(map[string]ExpirationVal),
		aof:   nil,
		Stats: NewStats(),
	}
}

// 辅助函数，判断键是否过期并进行删除
func (db *StandaloneDB) IsExpired(key string, val ExpirationVal) bool {
	if val.ExpiresAt.IsZero() {
		return false
	}
	if time.Now().After(val.ExpiresAt) {
		db.mu.Lock()
		defer db.mu.Unlock()
		// 再次检查防止并发问题
		if currentVal, found := db.data[key]; found && currentVal.ExpiresAt == val.ExpiresAt {
			delete(db.data, key)
			atomic.AddInt64(&db.Stats.KeyspaceHits, 1)
			return true
		}
	}
	return false
}

// 只负责存数据，不再解析 []protocol.Value
func (db *StandaloneDB) Set(key string, val []byte) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data[key] = ExpirationVal{Val: val, ExpiresAt: time.Time{}}
}

func (db *StandaloneDB) Get(key string) ([]byte, bool) {
	db.mu.RLock()
	val, ok := db.data[key]
	db.mu.RUnlock()
	
	if !ok {
		atomic.AddInt64(&db.Stats.KeyspaceMisses, 1)
		return nil, false
	}

	if db.IsExpired(key, val) {
		return nil, false
	}

	atomic.AddInt64(&db.Stats.KeyspaceHits, 1)
	return val.Val, true
}

// 设置键的过期时间
func (db *StandaloneDB) SetExpiration(key string, ddl time.Time) (bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	val, ok := db.data[key]
	if !ok {
		return false, nil
	}

	val.ExpiresAt = ddl
	db.data[key] = val
	return true, nil
}

// 移除键的过期时间
func (db *StandaloneDB) RmExpiration(key string) (bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	val, ok := db.data[key]
	if !ok {
		return false, nil
	}
	if val.ExpiresAt.IsZero() {
		return false, nil
	}

	val.ExpiresAt = time.Time{}
	db.data[key] = val
	return true, nil
}

// 获取键的剩余生存时间
func (db *StandaloneDB) GetTTL(key string) (time.Duration, bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	val, ok := db.data[key]
	if !ok {
		// 需要重新检查键是否存在，如果存在且已过期，视为不存在
		if db.IsExpired(key, val) {
			return 0, false, nil
		}
		return 0, false, nil
	}

	if val.ExpiresAt.IsZero() {
		return 0, true, nil
	}
	ttl := val.ExpiresAt.Sub(time.Now())
	if ttl <= 0 {
		db.IsExpired(key, val)
		return 0, false, nil
	}

	return ttl, true, nil
}

// 启动定期清理任务
func (db *StandaloneDB) StartCleanTask() {
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			db.cleanupExpiredKeys()
		}
	}()
}

// 辅助函数，实现定期清理任务
func (db *StandaloneDB) cleanupExpiredKeys() {
	const sampleSize = 20
	const threshold = 0.25
	const maxLoops = 10
	for i:= 0; i < maxLoops; i++ {
		expiredCount := 0
		checkedCount := 0
		db.mu.Lock()

		for key, val := range db.data {
			if !val.ExpiresAt.IsZero() {
				if time.Now().After(val.ExpiresAt) {
					delete(db.data, key)
					atomic.AddInt64(&db.Stats.KeyspaceHits, 1)
					expiredCount++
				}
			}
			checkedCount++
			if checkedCount >= sampleSize {
				break
			}
		}
		db.mu.Unlock()
		if checkedCount == 0 {
			return
		}
		
		if float64(expiredCount)/float64(checkedCount) < threshold {
			break
		}
	}
}

// 返回统计信息
func (db *StandaloneDB) GetStats() *Stats {
	return db.Stats
}

// 返回键数量
func (db *StandaloneDB) KeyCount() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.data)
}

// 注入AOF引擎
func (db *StandaloneDB) SetAof(a *aof.Aof) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.aof = a
}

// 获取AOF引擎
func (db *StandaloneDB) GetAof() *aof.Aof {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.aof
}