package db

import (
	"godis/internal/aof"
	"godis/pkg/protocol"
	"strings"
	"sync"
)

type Database struct {
	data map[string]string
	mu   sync.RWMutex
	aof *aof.Aof
}

func NewDatabase() *Database {
	return &Database{
		data: make(map[string]string),
		aof:  nil, // 初始化时默认为 nil
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
	default:
		res = protocol.Value{Type: protocol.Error, Str: "ERR unknown command '" + commandName + "'"}
	}

	if commandName == "SET" && res.Type != protocol.Error && db.aof != nil {
		payload := toRespBytes(cmd)
		db.aof.Write((payload))
	}
	return res
}

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

func (db *Database) get(args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return protocol.Value{Type: protocol.Error, Str: "ERR wrong number of arguments for 'get' command"}
	}

	key := string(args[0].Bulk)

	db.mu.RLock()
	val, ok := db.data[key]
	db.mu.RUnlock()

	if !ok {
		return protocol.Value{Type: protocol.BulkString, Bulk: nil}
	}

	return protocol.Value{Type: protocol.BulkString, Bulk: []byte(val)}
}