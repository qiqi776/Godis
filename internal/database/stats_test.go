package database_test

import (
	"godis/internal/database"
	"testing"
)

func TestStats(t *testing.T) {
	db := database.NewStandalone()
	
	// 1. 执行直接存储操作
	// SET key val
	db.Set("key", []byte("val"))

	// 2. 执行读取 (命中)
	// GET key
	val, ok := db.Get("key") 
	if !ok || string(val) != "val" {
		t.Errorf("Expected val, got %s", val)
	}

	// 3. 执行读取 (未命中)
	// GET unknown
	_, ok = db.Get("unknown")
	if ok {
		t.Errorf("Expected not found")
	}

	// 4. 验证统计数据
	// TotalCommandsProcessed 现在是在 Server 层统计的，Database 层只统计 Hits/Misses
	stats := db.GetStats()

	if stats.KeyspaceHits != 1 {
		t.Errorf("Expected 1 hit, got %d", stats.KeyspaceHits)
	}
	if stats.KeyspaceMisses != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.KeyspaceMisses)
	}
	
	// 验证 Key 数量
	if db.KeyCount() != 1 {
		t.Errorf("Expected 1 key, got %d", db.KeyCount())
	}
}