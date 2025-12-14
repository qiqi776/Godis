package database_test

import (
	"godis/internal/database"
	"testing"
	"time"
)

// 测试 SetExpiration 和 GetTTL 的基本生命周期
func TestExpirationLifeCycle(t *testing.T) {
	db := database.NewStandalone()
	key := "test_ttl"
	
	// 1. 初始状态：键不存在，应该返回 (-2, false)
	_, found, _ := db.GetTTL(key)
	if found {
		t.Errorf("Expected key not found, got found")
	}

	// 2. SET：键存在但无过期时间，应该返回 (0, true) 即 -1 的语义
	db.Set(key, []byte("value"))
	duration, found, _ := db.GetTTL(key)
	if !found || duration != 0 {
		t.Errorf("Expected key found and no-expiration (0), got duration=%v, found=%t", duration, found)
	}

	// 3. SetExpiration：设置 10 秒过期
	deadline := time.Now().Add(10 * time.Second)
	ok, _ := db.SetExpiration(key, deadline)
	if !ok {
		t.Fatalf("Expected SetExpiration success")
	}

	// 4. GetTTL：检查剩余时间是否在合理范围内
	duration, found, _ = db.GetTTL(key)
	if !found || duration <= 8*time.Second || duration > 10*time.Second {
		t.Errorf("Expected TTL near 10s, got %v", duration)
	}

	// 5. RemoveExpiration (PERSIST)：移除过期时间
	removed, _ := db.RmExpiration(key)
	if !removed {
		t.Errorf("Expected RemoveExpiration success")
	}
	
	// 6. 再次检查 TTL，应该变回无过期时间
	duration, found, _ = db.GetTTL(key)
	if !found || duration != 0 {
		t.Errorf("Expected TTL=0 (no expiration) after removal, got %v", duration)
	}
}

// 测试惰性删除 (Lazy Deletion)
func TestLazyDeletion(t *testing.T) {
	db := database.NewStandalone()
	key := "expired_key"
	db.Set(key, []byte("will_expire"))

	// 设置 50ms 后过期
	deadline := time.Now().Add(50 * time.Millisecond)
	db.SetExpiration(key, deadline)

	// 立即 GET，应该能取到
	_, ok := db.Get(key)
	if !ok {
		t.Fatalf("Expected key to be found before expiration")
	}

	// 等待过期 (100ms > 50ms)
	time.Sleep(100 * time.Millisecond)

	// 触发惰性删除：Get 应该返回 false
	_, ok = db.Get(key)
	if ok {
		t.Fatalf("Expected key to be lazily deleted and not found")
	}
	
	// 验证数据库 Key 数量是否为 0
	if db.KeyCount() != 0 {
		t.Errorf("Expected 0 keys after lazy deletion, got %d", db.KeyCount())
	}
}

// 测试定期删除 (Periodic Deletion)
func TestPeriodicDeletion(t *testing.T) {
	db := database.NewStandalone()
	db.Set("key1", []byte("short")) // 将快速过期
	db.Set("key2", []byte("long"))  // 存活较久
	
	// key1: 100ms 后过期
	db.SetExpiration("key1", time.Now().Add(100*time.Millisecond))
	// key2: 10s 后过期
	db.SetExpiration("key2", time.Now().Add(10*time.Second))

	// 启动后台清理任务
	db.StartCleanTask()

	// 等待 1.2 秒 (足以覆盖 key1 过期 + 几轮清理周期)
	// 因为清理周期我们在 standalone.go 里设的是 100ms
	time.Sleep(1200 * time.Millisecond)
    
	// 检查 key1：应该已经被后台任务删除了
	_, ok := db.Get("key1")
	if ok {
		t.Errorf("Expected key1 to be cleaned up by periodic task, but it still exists")
	}
	
	// 检查 key2：应该还在
	_, ok = db.Get("key2")
	if !ok {
		t.Errorf("Expected key2 to survive")
	}
	
	// 验证最终数量
	if db.KeyCount() != 1 {
		t.Errorf("Expected 1 key remaining, got %d", db.KeyCount())
	}
}