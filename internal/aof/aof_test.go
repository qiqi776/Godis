package aof

import (
	"godis/pkg/protocol"
	"os"
	"testing"
)

func TestAofWriteAndRead(t *testing.T) {
	tmpFile := "test_aof_unit.aof"
	defer os.Remove(tmpFile)

	// 1. 创建 AOF
	aof, err := NewAof(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create AOF: %v", err)
	}

	// 2. 写入测试数据 (模拟 RESP 字节流)
	// SET key val -> *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n
	cmdBytes := []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n")
	if err := aof.Write(cmdBytes); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	aof.Close() // 关闭以落盘

	// 3. 读取验证
	reopenAof, err := NewAof(tmpFile)
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer reopenAof.Close()

	var count int
	err = reopenAof.Read(func(val protocol.Value) {
		count++
		// 简单验证解析出的命令是不是 Array 类型
		if val.Type != protocol.Array {
			t.Errorf("Expected Array type, got %v", val.Type)
		}
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 command, got %d", count)
	}
}