package database

import (
	"godis/internal/aof"
	"godis/pkg/protocol"
	"os"
	"strings"
	"testing"
)
// 辅助函数：生成 RESP 格式的 SET 命令字节流
func makeSetPayload(key, val string) []byte {
	var sb strings.Builder
	sb.WriteString("*3\r\n")
	sb.WriteString("$3\r\nSET\r\n")
	sb.WriteString(protocol.MakeBulkString(key))
	sb.WriteString(protocol.MakeBulkString(val))
	return []byte(sb.String())
}


// 模拟配置加载和 DB 初始化流程
func TestDatabase_Persistence_Integration(t *testing.T) {
	tmpFile := "test_db_persistence.aof"
	defer os.Remove(tmpFile)

	// --- 阶段 1: 启动服务器，写入数据，模拟崩溃（关闭） ---
	{
		// 初始化 DB
		db := NewStandalone()
		// 初始化 AOF (使用 everysec 策略)
		aofHandler, err := aof.NewAof(tmpFile, aof.FsyncEverySec)
		if err != nil {
			t.Fatalf("Failed to create AOF: %v", err)
		}
		// 注入 AOF (虽然我们在下面是手动 Write，但保持注入是好习惯)
		db.SetAof(aofHandler)

		// 准备测试数据
		cmds := []struct {
			key, val string
		}{
			{"name", "godis"},
			{"lang", "golang"},
			{"type", "kv-store"},
		}

		for _, c := range cmds {
			// 1. 模拟业务逻辑：写入内存
			db.Set(c.key, []byte(c.val))

			// 2. 模拟 Command 层行为：写入 AOF
			// 注意：在正式代码中，这是由 internal/commands/string.go 完成的
			// 在单元测试中，我们手动构建 payload 并写入，以避免循环依赖
			payload := makeSetPayload(c.key, c.val)
			if err := aofHandler.Write(payload); err != nil {
				t.Errorf("AOF Write failed: %v", err)
			}
		}

		// 模拟服务器关闭 (Close 会强制落盘)
		aofHandler.Close()
	}

	// --- 阶段 2: 服务器重启，从 AOF 恢复数据 ---
	{
		// 创建新的空 DB
		dbRestart := NewStandalone()

		// 打开现有的 AOF 文件用于恢复
		recoveryAof, err := aof.NewAof(tmpFile, aof.FsyncNo)
		if err != nil {
			t.Fatalf("Failed to open AOF for recovery: %v", err)
		}

		// 恢复数据
		cmdCount := 0
		err = recoveryAof.Read(func(cmd protocol.Value) {
			// 这里我们手动解析 RESP 数据并重放到 DB 中
			// 类似于 main.go 中的逻辑，但这里我们只处理 SET 命令
			if cmd.Type == protocol.Array && len(cmd.Array) >= 3 {
				cmdName := strings.ToUpper(string(cmd.Array[0].Bulk))
				if cmdName == "SET" {
					key := string(cmd.Array[1].Bulk)
					val := cmd.Array[2].Bulk
					dbRestart.Set(key, val)
					cmdCount++
				}
			}
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
		checkKey(t, dbRestart, "type", "kv-store")

		recoveryAof.Close()
	}
}

// 辅助函数：检查 DB 中的 Key
func checkKey(t *testing.T, db *StandaloneDB, key, expectedVal string) {
	val, ok := db.Get(key)
	if !ok {
		t.Errorf("Key %s not found", key)
		return
	}
	if string(val) != expectedVal {
		t.Errorf("Get %s: expected %s, got %s", key, expectedVal, val)
	}
}