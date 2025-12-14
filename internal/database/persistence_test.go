package database

import (
	"fmt"
	"godis/internal/aof"
	"godis/pkg/protocol"
	"os"
	"strings"
	"testing"
	"time"
)

// 辅助函数：生成 RESP 格式的 EXPIRE 命令字节流
// 模拟 Command 层行为
func makeExpirePayload(key string, seconds int) []byte {
	// *3\r\n$6\r\nEXPIRE\r\n$len\r\nkey\r\n$len\r\nseconds\r\n
	var sb strings.Builder
	sb.WriteString(protocol.MakeArrayHeader(3))
	sb.WriteString(protocol.MakeBulkString("EXPIRE"))
	sb.WriteString(protocol.MakeBulkString(key))
	sb.WriteString(protocol.MakeBulkString(fmt.Sprintf("%d", seconds)))
	return []byte(sb.String())
}

// 辅助函数：生成 SET 命令
func makeSetPayload(key, val string) []byte {
	var sb strings.Builder
	sb.WriteString(protocol.MakeArrayHeader(3))
	sb.WriteString(protocol.MakeBulkString("SET"))
	sb.WriteString(protocol.MakeBulkString(key))
	sb.WriteString(protocol.MakeBulkString(val))
	return []byte(sb.String())
}

func TestDatabase_Persistence_WithTTL(t *testing.T) {
	tmpFile := "test_ttl_persistence.aof"
	defer os.Remove(tmpFile)

	// --- Phase 1: 写入数据和过期时间 ---
	{
		db := NewStandalone()
		aofHandler, err := aof.NewAof(tmpFile, aof.FsyncEverySec)
		if err != nil { t.Fatalf("AOF init failed: %v", err) }
		db.SetAof(aofHandler)

		// 1. SET key
		db.Set("key_ttl", []byte("val_ttl"))
		aofHandler.Write(makeSetPayload("key_ttl", "val_ttl"))

		// 2. EXPIRE key 100 (模拟命令执行成功后写入 AOF)
		deadline := time.Now().Add(100 * time.Second)
		db.SetExpiration("key_ttl", deadline)
		aofHandler.Write(makeExpirePayload("key_ttl", 100))

		aofHandler.Close()
	}

	// --- Phase 2: 恢复数据 ---
	{
		dbRestart := NewStandalone()
		recoveryAof, err := aof.NewAof(tmpFile, aof.FsyncNo)
		if err != nil { t.Fatalf("Recovery init failed: %v", err) }

		// 模拟 main.go 中的命令重放逻辑
		// 注意：这里需要手动解析 SET 和 EXPIRE
		err = recoveryAof.Read(func(cmd protocol.Value) {
			if cmd.Type != protocol.Array || len(cmd.Array) < 2 { return }
			
			cmdName := strings.ToUpper(string(cmd.Array[0].Bulk))
			key := string(cmd.Array[1].Bulk)

			if cmdName == "SET" {
				val := cmd.Array[2].Bulk
				dbRestart.Set(key, val)
			} else if cmdName == "EXPIRE" {
				// 简单的重放逻辑：读取秒数并设置
				// 实际项目中这里是调用 commands.Execute，我们这里仅做单元测试模拟
				secondsStr := string(cmd.Array[2].Bulk)
				var seconds int
				fmt.Sscanf(secondsStr, "%d", &seconds)
				dbRestart.SetExpiration(key, time.Now().Add(time.Duration(seconds)*time.Second))
			}
		})
		if err != nil { t.Fatalf("Read failed: %v", err) }

		// 验证数据
		val, ok := dbRestart.Get("key_ttl")
		if !ok || string(val) != "val_ttl" {
			t.Errorf("Data lost after recovery")
		}

		// 验证 TTL 是否存在
		ttl, found, _ := dbRestart.GetTTL("key_ttl")
		if !found {
			t.Errorf("TTL lost after recovery")
		}
		if ttl <= 0 {
			t.Errorf("Key should not be expired yet")
		}
		
		recoveryAof.Close()
	}
}