package db

import (
	"godis/pkg/protocol"
	"strings"
	"sync"
)

type Database struct {
	data map[string]string
	mu   sync.RWMutex
}

func NewDatabase() *Database {
	return &Database{
		data: make(map[string]string),
	}
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

	switch commandName {
	case "PING":
		return protocol.Value{Type: protocol.SimpleString, Str: "PONG"}
	case "SET":
		return db.set(args)
	case "GET":
		return db.get(args)
	default:
		return protocol.Value{Type: protocol.Error, Str: "ERR unknown command '" + commandName + "'"}
	}
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