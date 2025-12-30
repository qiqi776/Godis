package database_test

import (
	"godis/internal/core"
	"godis/internal/database"
	"testing"
	"time"
)

func TestExpirationLifeCycle(t *testing.T) {
	db := database.NewStandalone()
	key := "test_ttl"
	_, found, _ := db.GetTTL(0, key)
	if found {
		t.Errorf("Expected key not found, got found")
	}
	db.Set(0, key, &core.RedisObject{
		Type: core.ObjectTypeString,
		Ptr:  []byte("value"),
	})
	duration, found, _ := db.GetTTL(0, key)
	if !found || duration != 0 {
		t.Errorf("Expected key found and no-expiration (0), got duration=%v, found=%t", duration, found)
	}
	deadline := time.Now().Add(10 * time.Second)
	ok, _ := db.SetExpiration(0, key, deadline)
	if !ok {
		t.Fatalf("Expected SetExpiration success")
	}
	duration, found, _ = db.GetTTL(0, key)
	if !found || duration <= 8*time.Second || duration > 10*time.Second {
		t.Errorf("Expected TTL near 10s, got %v", duration)
	}
	removed, _ := db.RmExpiration(0, key)
	if !removed {
		t.Errorf("Expected RemoveExpiration success")
	}
	duration, found, _ = db.GetTTL(0, key)
	if !found || duration != 0 {
		t.Errorf("Expected TTL=0 (no expiration) after removal, got %v", duration)
	}
}

func TestLazyDeletion(t *testing.T) {
	db := database.NewStandalone()
	key := "expired_key"
	db.Set(0, key, &core.RedisObject{
		Type: core.ObjectTypeString,
		Ptr:  []byte("will_expire"),
	})
	deadline := time.Now().Add(50 * time.Millisecond)
	db.SetExpiration(0, key, deadline)
	_, ok := db.Get(0, key)
	if !ok {
		t.Fatalf("Expected key to be found before expiration")
	}
	time.Sleep(100 * time.Millisecond)
	_, ok = db.Get(0, key)
	if ok {
		t.Fatalf("Expected key to be lazily deleted and not found")
	}
	if db.KeyCount(0) != 0 {
		t.Errorf("Expected 0 keys after lazy deletion, got %d", db.KeyCount(0))
	}
}

func TestPeriodicDeletion(t *testing.T) {
	db := database.NewStandalone()
	db.Set(0, "key1", &core.RedisObject{
		Type: core.ObjectTypeString,
		Ptr:  []byte("short"),
	})
	db.Set(0, "key2", &core.RedisObject{
		Type: core.ObjectTypeString,
		Ptr:  []byte("long"),
	})
	db.SetExpiration(0, "key1", time.Now().Add(100*time.Millisecond))
	db.SetExpiration(0, "key2", time.Now().Add(10*time.Second))
	db.StartCleanTask()
	time.Sleep(1200 * time.Millisecond)
	_, ok := db.Get(0, "key1")
	if ok {
		t.Errorf("Expected key1 to be cleaned up by periodic task, but it still exists")
	}
	_, ok = db.Get(0, "key2")
	if !ok {
		t.Errorf("Expected key2 to survive")
	}
	if db.KeyCount(0) != 1 {
		t.Errorf("Expected 1 key remaining, got %d", db.KeyCount(0))
	}
}