package db

import (
	"godis/internal/aof"
	"godis/pkg/protocol"
	"os"
	"testing"
)

// 模拟配置加载和 DB 初始化流程
func TestDatabase_Persistence_Integration(t *testing.T) {
	tmpFile := "test_db_persistence.aof"
	defer os.Remove(tmpFile)

	// --- 阶段 1: 启动服务器，写入数据，模拟崩溃（关闭） ---
	{
		// 初始化 DB
		db := NewDatabase()
		// 初始化 AOF (使用 everysec 策略)
		aofHandler, err := aof.NewAof(tmpFile, aof.FsyncEverySec)
		if err != nil {
			t.Fatalf("Failed to create AOF: %v", err)
		}
		db.SetAof(aofHandler)

		// 执行一些写入命令
		cmds := []struct {
			key, val string
		}{
			{"name", "godis"},
			{"lang", "golang"},
			{"type", "kv-store"},
		}

		for _, c := range cmds {
			// 构造类似 SET key val 的命令
			cmdArgs := []protocol.Value{
				{Type: protocol.BulkString, Bulk: []byte("SET")},
				{Type: protocol.BulkString, Bulk: []byte(c.key)},
				{Type: protocol.BulkString, Bulk: []byte(c.val)},
			}
			cmd := protocol.Value{Type: protocol.Array, Array: cmdArgs}
			
			// 执行命令 (Exec 内部会自动调用 aof.Write)
			res := db.Exec(cmd)
			if string(res.Str) != "OK" {
				t.Errorf("Exec failed: %v", res)
			}
		}

		// 模拟服务器关闭 (Close 会强制落盘)
		aofHandler.Close()
	}

	// --- 阶段 2: 服务器重启，从 AOF 恢复数据 ---
	{
		// 创建新的空 DB
		dbRestart := NewDatabase()
		
		// 打开现有的 AOF 文件
		// 注意：恢复时通常不需要后台 fsync 协程，所以策略可以随意，或者只用 Read 功能
		recoveryAof, err := aof.NewAof(tmpFile, aof.FsyncNo)
		if err != nil {
			t.Fatalf("Failed to open AOF for recovery: %v", err)
		}

		// 恢复数据
		cmdCount := 0
		err = recoveryAof.Read(func(val protocol.Value) {
			dbRestart.Exec(val)
			cmdCount++
		})
		if err != nil {
			t.Fatalf("Recovery failed: %v", err)
		}

		if cmdCount != 3 {
			t.Errorf("Expected to recover 3 commands, got %d", cmdCount)
		}

		// 验证内存中的数据是否正确
		checkKey(t, dbRestart, "name", "godis")
		checkKey(t, dbRestart, "lang", "golang")
		
		recoveryAof.Close()
	}
}

// 辅助函数：检查 DB 中的 Key
func checkKey(t *testing.T, db *Database, key, expectedVal string) {
	// 构造 GET 命令
	cmdArgs := []protocol.Value{
		{Type: protocol.BulkString, Bulk: []byte("GET")},
		{Type: protocol.BulkString, Bulk: []byte(key)},
	}
	cmd := protocol.Value{Type: protocol.Array, Array: cmdArgs}

	res := db.Exec(cmd)
	if string(res.Bulk) != expectedVal {
		t.Errorf("Get %s: expected %s, got %s", key, expectedVal, res.Bulk)
	}
}