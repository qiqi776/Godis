package db_test

import (
	"godis/internal/db"
	"godis/pkg/protocol"
	"strings"
	"testing"
)

func TestStats(t *testing.T) {
	database := db.NewDatabase()
	
	// 1. 执行一些命令
	setCmd := protocol.Value{Type: protocol.Array, Array: []protocol.Value{
		{Type: protocol.BulkString, Bulk: []byte("SET")},
		{Type: protocol.BulkString, Bulk: []byte("key")},
		{Type: protocol.BulkString, Bulk: []byte("val")},
	}}
	database.Exec(setCmd)

	getCmd := protocol.Value{Type: protocol.Array, Array: []protocol.Value{
		{Type: protocol.BulkString, Bulk: []byte("GET")},
		{Type: protocol.BulkString, Bulk: []byte("key")},
	}}
	database.Exec(getCmd) // Hit

	getMissCmd := protocol.Value{Type: protocol.Array, Array: []protocol.Value{
		{Type: protocol.BulkString, Bulk: []byte("GET")},
		{Type: protocol.BulkString, Bulk: []byte("unknown")},
	}}
	database.Exec(getMissCmd) // Miss

	// 2. 获取 INFO
	infoCmd := protocol.Value{Type: protocol.Array, Array: []protocol.Value{
		{Type: protocol.BulkString, Bulk: []byte("INFO")},
	}}
	res := database.Exec(infoCmd)

	infoStr := string(res.Bulk)
	
	// 3. 验证统计数据
	if !strings.Contains(infoStr, "total_commands_processed:4") { // SET+GET+GET+INFO = 4
		t.Errorf("Expected 4 commands processed, got response: %s", infoStr)
	}
	if !strings.Contains(infoStr, "keyspace_hits:1") {
		t.Errorf("Expected 1 hit")
	}
	if !strings.Contains(infoStr, "keyspace_misses:1") {
		t.Errorf("Expected 1 miss")
	}
}