package database

import (
	"godis/internal/aof"
	"godis/internal/core"
	"sync"
	"sync/atomic"
	"time"
)

// ExpirationVal 存储带有过期时间的值
type ExpirationVal struct {
	Val       *core.RedisObject 
	ExpiresAt time.Time
}

type database struct {
	data map[string]ExpirationVal
	mu   sync.RWMutex
}

func newDatabase() *database {
	return &database{
		data: make(map[string]ExpirationVal),
	}
}

type StandaloneDB struct {
	dbs   []*database
	aof   *aof.Aof
	aofmu sync.RWMutex
	Stats *core.Stats
	dbNum int
}

func NewStandalone() *StandaloneDB {
	dbNum := 16
	dbs := make([]*database, dbNum)
	for i := 0; i < dbNum; i++ {
		dbs[i] = newDatabase()
	}
	return &StandaloneDB{
		dbs:  dbs,
		aof:   nil,
		Stats: core.NewStats(),
		dbNum: dbNum,
	}
}

// 辅助方法：获取指定索引的数据库
func (db *StandaloneDB) mustGetDB(index int) *database {
	if index < 0 || index >= db.dbNum {
		panic("database index out of range")
	}
	return db.dbs[index]
}

func (db *StandaloneDB) IsValidDBIndex(index int) bool {
	return index >= 0 && index < db.dbNum
}

// isExpired 判断键是否过期并进行删除
func (db *StandaloneDB) isExpired(d *database, key string, val ExpirationVal) bool {
	if val.ExpiresAt.IsZero() {
		return false
	}
	if time.Now().After(val.ExpiresAt) {
		d.mu.Lock()
		defer d.mu.Unlock()
		if currentVal, found := d.data[key]; found && currentVal.ExpiresAt == val.ExpiresAt {
			delete(d.data, key)
			atomic.AddInt64(&db.Stats.KeyspaceHits, 1)
			return true
		}
	}
	return false
}

// Set 存储 Key-Value
func (db *StandaloneDB) Set(dbIndex int, key string, val *core.RedisObject) {
	d := db.mustGetDB(dbIndex)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.data[key] = ExpirationVal{Val: val, ExpiresAt: time.Time{}}
}

// Get 获取 Value
func (db *StandaloneDB) Get(dbIndex int, key string) (*core.RedisObject, bool) {
	d := db.mustGetDB(dbIndex)
	d.mu.RLock()
	val, ok := d.data[key]
	d.mu.RUnlock()

	if !ok {
		atomic.AddInt64(&db.Stats.KeyspaceMisses, 1)
		return nil, false
	}

	if db.isExpired(d, key, val) {
		return nil, false
	}

	atomic.AddInt64(&db.Stats.KeyspaceHits, 1)
	return val.Val, true
}

// Remove 删除 Key (为 DEL 命令支持)
func (db *StandaloneDB) Remove(dbIndex int, key string) int {
	d := db.mustGetDB(dbIndex)
	d.mu.Lock()
	defer d.mu.Unlock()
	_, exists := d.data[key]
	delete(d.data, key)
	if exists {
		atomic.AddInt64(&db.Stats.KeyspaceHits, 1)
		return 1
	}
	atomic.AddInt64(&db.Stats.KeyspaceMisses, 1)
	return 0
}
// SetExpiration 设置过期时间
func (db *StandaloneDB) SetExpiration(dbIndex int, key string, ddl time.Time) (bool, error) {
	d := db.mustGetDB(dbIndex)
	d.mu.Lock()
	defer d.mu.Unlock()
	val, ok := d.data[key]
	if !ok {
		return false, nil
	}
	val.ExpiresAt = ddl
	d.data[key] = val
	return true, nil
}

// RmExpiration 移除过期时间 (PERSIST)
func (db *StandaloneDB) RmExpiration(dbIndex int, key string) (bool, error) {
	d := db.mustGetDB(dbIndex)
	d.mu.Lock()
	defer d.mu.Unlock()
	val, ok := d.data[key]
	if !ok || val.ExpiresAt.IsZero() {
		return false, nil
	}
	val.ExpiresAt = time.Time{}
	d.data[key] = val
	return true, nil
}

// GetTTL 获取剩余生存时间
func (db *StandaloneDB) GetTTL(dbIndex int, key string) (time.Duration, bool, error) {
	d := db.mustGetDB(dbIndex)
	d.mu.Lock()
	defer d.mu.Unlock()
	val, ok := d.data[key]
	if !ok {
		return 0, false, nil
	}
	
	if !val.ExpiresAt.IsZero() && time.Now().After(val.ExpiresAt) {
		// 这里简化处理，不立即删除，只返回不存在
		return 0, false, nil
	}

	if val.ExpiresAt.IsZero() {
		return 0, true, nil
	}
	ttl := val.ExpiresAt.Sub(time.Now())
	return ttl, true, nil
}

func (db *StandaloneDB) KeyCount(dbIndex int) int {
	d := db.mustGetDB(dbIndex)
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.data)
}

func (db *StandaloneDB) GetStats() *core.Stats {
	return db.Stats
}


func (db *StandaloneDB) SetAof(a *aof.Aof) {
	db.aofmu.Lock()
	defer db.aofmu.Unlock()
	db.aof = a
}

func (db *StandaloneDB) GetAof() *aof.Aof {
	db.aofmu.RLock()
	defer db.aofmu.RUnlock()
	return db.aof
}

// StartCleanTask 定期清理任务
func (db *StandaloneDB) StartCleanTask() {
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			for i := 0; i < db.dbNum; i++ {
				db.cleanupExpiredKeys(i)
			}
		}
	}()
}

// cleanupExpiredKeys 实现贪婪过期算法
func (db *StandaloneDB) cleanupExpiredKeys(index int) {
	d := db.dbs[index]
	const sampleSize = 20
	const threshold = 0.25
	const maxLoops = 10
	for i := 0; i < maxLoops; i++ {
		expiredCount := 0
		checkedCount := 0
		d.mu.Lock()
		for key, val := range d.data {
			if !val.ExpiresAt.IsZero() {
				if time.Now().After(val.ExpiresAt) {
					delete(d.data, key)
					expiredCount++
					atomic.AddInt64(&db.Stats.KeyspaceHits, 1)
				}
			}
			checkedCount++
			if checkedCount >= sampleSize {
				break
			}
		}
		d.mu.Unlock()
		if checkedCount == 0 || float64(expiredCount)/float64(checkedCount) < threshold {
			break
		}
	}
}

// FlushDB 清空指定数据库
func (db *StandaloneDB) FlushDB(dbIndex int) {
    d := db.mustGetDB(dbIndex)
    d.mu.Lock()
    defer d.mu.Unlock()
    d.data = make(map[string]ExpirationVal)
}

// FlushAll 清空所有数据库
func (db *StandaloneDB) FlushAll() {
    for i := 0; i < db.dbNum; i++ {
        db.FlushDB(i)
    }
}

// Keys 返回匹配 pattern 的所有 key
func (db *StandaloneDB) Keys(dbIndex int, pattern string) []string {
    d := db.mustGetDB(dbIndex)
    d.mu.RLock()
    defer d.mu.RUnlock()

    keys := make([]string, 0, len(d.data))
    for k := range d.data {
        if pattern == "*" {
            keys = append(keys, k)
        } else {
             // TODO
        }
    }
    return keys
}

// Close 关闭数据库
func (db *StandaloneDB) Close() {
	db.aofmu.Lock()
	defer db.aofmu.Unlock()
	if db.aof != nil {
		db.aof.Close()
	}
}