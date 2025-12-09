package aof

import (
	"fmt"
	"godis/pkg/protocol"
	"os"
	"sync"
	"testing"
	"time"
)

// 辅助函数：构建 SET key val 的 RESP 字节流
func makeSetCmd(key, val string) []byte {
	// 简单的 RESP 构造：*3\r\n$3\r\nSET\r\n$len\r\nkey\r\n$len\r\nval\r\n
	return []byte(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val))
}

// 测试三种策略的基本写入和读取能力
func TestAof_Strategies(t *testing.T) {
	tests := []struct {
		name     string
		strategy string
	}{
		{"Strategy_Always", FsyncAlways},
		{"Strategy_EverySec", FsyncEverySec},
		{"Strategy_No", FsyncNo},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpFile := fmt.Sprintf("test_aof_%s.aof", tt.strategy)
			defer os.Remove(tmpFile)

			// 1. 初始化 AOF
			aof, err := NewAof(tmpFile, tt.strategy)
			if err != nil {
				t.Fatalf("Failed to create AOF: %v", err)
			}

			// 2. 写入数据
			cmd := makeSetCmd("key1", "value1")
			if err := aof.Write(cmd); err != nil {
				t.Errorf("Write failed: %v", err)
			}

			// 3. 关闭 AOF (对于 everysec 和 no，Close 操作至关重要，它保证了缓冲区数据落盘)
			if err := aof.Close(); err != nil {
				t.Errorf("Close failed: %v", err)
			}

			// 4. 重新打开并验证数据
			verifyAofContent(t, tmpFile, 1)
		})
	}
}

// 专门测试 EverySec 策略的优雅关闭
// 验证：即使定时器还没触发，Close() 也必须强制刷盘，防止丢失最近 1 秒的数据
func TestAof_EverySec_GracefulClose(t *testing.T) {
	tmpFile := "test_aof_graceful.aof"
	defer os.Remove(tmpFile)

	aof, err := NewAof(tmpFile, FsyncEverySec)
	if err != nil {
		t.Fatalf("Failed to create AOF: %v", err)
	}

	// 写入数据
	cmd := makeSetCmd("graceful", "check")
	aof.Write(cmd)

	// 不等待 1 秒，立即关闭
	// 如果 Close() 没有实现最后一次 Sync()，这里的数据就会丢失
	aof.Close()

	// 验证数据是否存在
	verifyAofContent(t, tmpFile, 1)
}

// 并发写入测试 (Race Condition Check)
// 模拟高并发下 AOF 模块是否稳定（Write 和 backgroundFsync 是否冲突）
func TestAof_ConcurrentWrite(t *testing.T) {
	tmpFile := "test_aof_concurrent.aof"
	defer os.Remove(tmpFile)

	aof, err := NewAof(tmpFile, FsyncEverySec)
	if err != nil {
		t.Fatalf("Failed to create AOF: %v", err)
	}

	var wg sync.WaitGroup
	count := 100

	// 启动 100 个协程并发写入
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", idx)
			val := fmt.Sprintf("v%d", idx)
			aof.Write(makeSetCmd(key, val))
		}(i)
	}

	wg.Wait()
	
	// 等待一小会儿确保后台协程有机会运行（虽然不是必须的，但能增加覆盖率）
	time.Sleep(100 * time.Millisecond)
	
	aof.Close()

	// 验证一共写入了 100 条命令
	verifyAofContent(t, tmpFile, count)
}

// 辅助验证函数
func verifyAofContent(t *testing.T, path string, expectedCount int) {
	// 重新打开用于读取
	aof, err := NewAof(path, FsyncNo)
	if err != nil {
		t.Fatalf("Failed to reopen verification AOF: %v", err)
	}
	defer aof.Close()

	count := 0
	err = aof.Read(func(val protocol.Value) {
		count++
		if val.Type != protocol.Array {
			t.Errorf("Expected Array type command, got %v", val.Type)
		}
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if count != expectedCount {
		t.Errorf("Expected %d commands, got %d", expectedCount, count)
	}
}