package db

import (
	"godis/internal/aof"
	"godis/pkg/protocol"
	"strings"
	"sync"
	"sync/atomic"
)

type Database struct {
	data    map[string]string
	mu      sync.RWMutex
	aof    *aof.Aof
	Stats *Stats
}

func NewDatabase() *Database {
	return &Database{
		data:  make(map[string]string),
		aof:   nil, // 初始化时默认为 nil
		Stats: NewStats(),
	}
}

// 在数据恢复完成后注入 AOF 引擎
func (db *Database) SetAof(aof *aof.Aof) {
    db.mu.Lock()
    defer db.mu.Unlock()
    db.aof = aof
}

// 辅助函数，将Value转换回resp字节流
func toRespBytes(cmd protocol.Value) []byte {
	if cmd.Type != protocol.Array {
		return nil
	}

	var sb strings.Builder

	sb.WriteString(protocol.MakeArrayHeader(len(cmd.Array)))
	for _, v := range cmd.Array {
		sb.WriteString(protocol.MakeBulkString(string(v.Bulk)))
	}
	return []byte(sb.String())
}

// Exec 执行命令
func (db *Database) Exec(cmd protocol.Value) protocol.Value {
	if cmd.Type != protocol.Array {
		return protocol.Value{Type: protocol.Error, Str: "ERR command must be array"}
	}

	if len(cmd.Array) == 0 {
		return protocol.Value{Type: protocol.Error, Str: "ERR empty command"}
	}

	atomic.AddInt64(&db.Stats.TotalCommandsProcessed, 1)

	// 获取命令
	commandName := strings.ToUpper(string(cmd.Array[0].Bulk))
	args := cmd.Array[1:]

	var res protocol.Value
	switch commandName {
	case "PING":
		res = protocol.Value{Type: protocol.SimpleString, Str: "PONG"}
	case "SET":
		res = db.set(args)
	case "GET":
		res = db.get(args)
	case "INFO":
		res = db.handleInfo()
	default:
		res = protocol.Value{Type: protocol.Error, Str: "ERR unknown command '" + commandName + "'"}
	}

	if commandName == "SET" && res.Type != protocol.Error && db.aof != nil {
		payload := toRespBytes(cmd)
		db.aof.Write((payload))
	}
	return res
}

// 处理INFO命令
func (db *Database) handleInfo() protocol.Value {
	db.mu.RLock()
	keyCount := len(db.data)
	db.mu.RUnlock()

	infoStr := db.Stats.GetInfo(keyCount)
	return protocol.Value{Type: protocol.BulkString, Bulk: []byte(infoStr)}
}

// 写操作：需要Lock，执行写入，返回 OK
func (db *Database) set(args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return protocol.Value{Type: protocol.Error, Str: "ERR wrong number of arguments for 'set' command"}
	}

	key := string(args[0].Bulk)
	value := string(args[1].Bulk)

	db.mu.Lock()
	db.data[key] = value
	db.mu.Unlock()

	return protocol.Value{Type: protocol.SimpleString, Str: "OK"}
}

// 读操作：需要RLock，并统计Hits/Misses
func (db *Database) get(args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return protocol.Value{Type: protocol.Error, Str: "ERR wrong number of arguments for 'get' command"}
	}

	key := string(args[0].Bulk)

	db.mu.RLock()
	val, ok := db.data[key]
	db.mu.RUnlock()

	if !ok {
		// 统计未命中
		atomic.AddInt64(&db.Stats.KeyspaceMisses, 1)
		return protocol.Value{Type: protocol.BulkString, Bulk: nil}
	}

	// 统计命中
	atomic.AddInt64(&db.Stats.KeyspaceHits, 1)
	return protocol.Value{Type: protocol.BulkString, Bulk: []byte(val)}
}