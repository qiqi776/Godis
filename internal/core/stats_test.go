package core_test

import (
	"godis/internal/core"
	"godis/internal/database"
	"testing"
)

func TestStats(t *testing.T) {
	db := database.NewStandalone()
	db.Set(0, "key", &core.RedisObject{
		Type: core.ObjectTypeString,
		Ptr:  []byte("val"),
	})
	obj, ok := db.Get(0, "key")
	if !ok {
		t.Fatalf("Expected key found")
	}
	valBytes, typeOk := obj.Ptr.([]byte)
	if !typeOk || string(valBytes) != "val" {
		t.Errorf("Expected val, got %v", obj.Ptr)
	}
	_, ok = db.Get(0, "unknown")
	if ok {
		t.Errorf("Expected not found")
	}
	stats := db.GetStats()
	if stats.KeyspaceHits != 1 {
		t.Errorf("Expected 1 hit, got %d", stats.KeyspaceHits)
	}
	if stats.KeyspaceMisses != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.KeyspaceMisses)
	}
	if db.KeyCount(0) != 1 {
		t.Errorf("Expected 1 key, got %d", db.KeyCount(0))
	}
}