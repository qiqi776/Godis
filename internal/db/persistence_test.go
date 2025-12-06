package db

import (
	"godis/internal/aof"
	"godis/pkg/protocol"
	"os"
	"testing"
)

// 辅助函数保持不变...
func makeSetCmd(key, val string) protocol.Value {
	return protocol.Value{
		Type: protocol.Array,
		Array: []protocol.Value{
			{Type: protocol.BulkString, Bulk: []byte("SET")},
			{Type: protocol.BulkString, Bulk: []byte(key)},
			{Type: protocol.BulkString, Bulk: []byte(val)},
		},
	}
}

func makeGetCmd(key string) protocol.Value {
	return protocol.Value{
		Type: protocol.Array,
		Array: []protocol.Value{
			{Type: protocol.BulkString, Bulk: []byte("GET")},
			{Type: protocol.BulkString, Bulk: []byte(key)},
		},
	}
}

func TestPersistenceIntegration(t *testing.T) {
	aofFile := "db_integration_test.aof"
	defer os.Remove(aofFile)

	// --- 阶段 1: 写入数据 ---
	{
		engine, err := aof.NewAof(aofFile)
		if err != nil {
			t.Fatalf("Init AOF failed: %v", err)
		}

		database := NewDatabase() // 现在是无参调用，正确
		database.SetAof(engine)

		// 写入 key: "mykey" -> "persist_data"
		res := database.Exec(makeSetCmd("mykey", "persist_data"))
		if string(res.Str) != "OK" {
			t.Fatalf("SET command failed: %v", res.Str)
		}

		engine.Close()
	}

	// --- 阶段 2: 恢复数据 ---
	{
		engine, err := aof.NewAof(aofFile)
		if err != nil {
			t.Fatalf("Reopen AOF failed: %v", err)
		}
		defer engine.Close()

		database := NewDatabase()

		// 重放 AOF
		err = engine.Read(func(cmd protocol.Value) {
			database.Exec(cmd)
		})
		if err != nil {
			t.Fatalf("AOF replay failed: %v", err)
		}

		database.SetAof(engine)

		// [修正] 验证 "mykey" 是否存在 (对应阶段1的写入)
		res := database.Exec(makeGetCmd("mykey"))
		
		if res.Type == protocol.BulkString && res.Bulk == nil {
			t.Fatalf("Key 'mykey' not found (Recover failed)")
		}
		
		if string(res.Bulk) != "persist_data" {
			t.Errorf("Recover failed. Expected 'persist_data', got '%s'", string(res.Bulk))
		}

		// 验证不存在的 key
		res = database.Exec(makeGetCmd("missing_key"))
		if res.Bulk != nil {
			t.Errorf("Expected nil for missing key")
		}
	}
}