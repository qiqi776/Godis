package commands

import (
	"godis/internal/core"
	"godis/pkg/protocol"
	"strings"
)

// 定义命令标志
const (
	FlagWrite    = 1 << iota // 写命令
	FlagReadOnly             // 只读命令
)

// CmdLine 定义为字节切片的切片，用于表示一条命令
type CmdLine = [][]byte

// UndoFunc 定义撤销函数的签名
type UndoFunc func(ctx *core.Context) []CmdLine

// Command 结构体包含命令的所有元数据
type Command struct {
	Name     string
	Executor core.CommandFunc // 执行函数
	Undoer   UndoFunc         // 撤销/回滚函数
	Arity    int              // 参数数量限制 (例如: 2 表示必须2个参数; -2 表示 >= 2 个参数)
	Flags    int              // 命令标志 (读/写)
}

// 全局命令注册表
var Commands = make(map[string]*Command)

// Register 注册新命令
func Register(name string, executor core.CommandFunc, undoer UndoFunc, arity int, flags int) {
	name = strings.ToUpper(name)
	Commands[name] = &Command{
		Name:     name,
		Executor: executor,
		Undoer:   undoer,
		Arity:    arity,
		Flags:    flags,
	}
}

// Lookup 查找命令
func Lookup(name string) (*Command, bool) {
	cmd, ok := Commands[strings.ToUpper(name)]
	return cmd, ok
}

// Init 初始化并注册所有命令
func Init() {
	// System
	Register("PING", Ping, nil, -1, FlagReadOnly)
	Register("INFO", Info, nil, -1, FlagReadOnly)
	Register("TTL", TTL, nil, 2, FlagReadOnly)
	Register("PTTL", PTTL, nil, 2, FlagReadOnly)
	Register("EXPIRE", Expire, nil, 3, FlagWrite)
	Register("PEXPIRE", PExpire, nil, 3, FlagWrite)
	Register("PERSIST", Persist, nil, 2, FlagWrite)
	Register("SELECT", Select, nil, 2, FlagReadOnly)

	Register("DEL", Del, UndoDel, -2, FlagWrite)      
	Register("EXISTS", Exists, nil, -2, FlagReadOnly) 
	Register("TYPE", Type, nil, 2, FlagReadOnly) 
	// Manager
	Register("KEYS", Keys, nil, 2, FlagReadOnly)
    Register("FLUSHDB", FlushDB, nil, -1, FlagWrite)
    Register("FLUSHALL", FlushAll, nil, -1, FlagWrite)
    Register("DBSIZE", DBSize, nil, -1, FlagReadOnly)

	// String
    Register("SET", Set, UndoSet, -3, FlagWrite) 
    Register("GET", Get, nil, 2, FlagReadOnly)
    Register("SETNX", SetNX, UndoSetNX, 3, FlagWrite)
    Register("GETSET", GetSet, UndoGetSet, 3, FlagWrite)
    Register("STRLEN", StrLen, nil, 2, FlagReadOnly)
    Register("MSET", MSet, UndoMSet, -3, FlagWrite)
    Register("MGET", MGet, nil, -2, FlagReadOnly)

	// Bitmap
    Register("SETBIT", SetBit, UndoSetBit, 4, FlagWrite)
    Register("GETBIT", GetBit, nil, 3, FlagReadOnly)
    Register("BITCOUNT", BitCount, nil, -2, FlagReadOnly)
	
	// List
	Register("LPUSH", LPush, UndoLPush, -3, FlagWrite)
	Register("RPUSH", RPush, UndoRPush, -3, FlagWrite)
	Register("LPOP", LPop, UndoLPop, 2, FlagWrite)
	Register("RPOP", RPop, UndoRPop, 2, FlagWrite)
	Register("LRANGE", LRange, nil, 4, FlagReadOnly)
	Register("LLEN", LLen, nil, 2, FlagReadOnly)

	// Hash
    Register("HSET", HSet, UndoHSet, -4, FlagWrite)
    Register("HGET", HGet, nil, 3, FlagReadOnly)
    Register("HGETALL", HGetAll, nil, 2, FlagReadOnly)
    Register("HDEL", HDel, UndoHDel, -3, FlagWrite)
    Register("HEXISTS", HExists, nil, 3, FlagReadOnly)
    Register("HLEN", HLen, nil, 2, FlagReadOnly)

	// Set
    Register("SADD", SAdd, UndoSAdd, -3, FlagWrite)
    Register("SREM", SRem, UndoSrem, -3, FlagWrite)
    Register("SISMEMBER", SIsMember, nil, 3, FlagReadOnly)
    Register("SMEMBERS", SMembers, nil, 2, FlagReadOnly)
    Register("SCARD", SCard, nil, 2, FlagReadOnly)
    Register("SPOP", SPop, nil, -2, FlagWrite) 
    Register("SRANDMEMBER", SRandMember, nil, -2, FlagReadOnly)

	// Sorted Set
    Register("ZADD", ZAdd, UndoZAdd, -4, FlagWrite) 
    Register("ZREM", ZRem, UndoZRem, -3, FlagWrite) 
    Register("ZSCORE", ZScore, nil, 3, FlagReadOnly)
    Register("ZCARD", ZCard, nil, 2, FlagReadOnly)
    Register("ZRANK", ZRank, nil, 3, FlagReadOnly)
    Register("ZREVRANK", ZRevRank, nil, 3, FlagReadOnly)
    Register("ZRANGE", ZRange, nil, -4, FlagReadOnly) 
    Register("ZREVRANGE", ZRevRange, nil, -4, FlagReadOnly)

	// Transactions
	Register("MULTI", Multi, nil, 1, FlagReadOnly)
	Register("EXEC", Exec, nil, 1, FlagReadOnly)
	Register("DISCARD", Discard, nil, 1, FlagReadOnly)

}

// Execute 执行命令的统一入口
func Execute(name string, ctx *core.Context) protocol.Value {
	cmd, ok := Lookup(name)
	if !ok {
		return protocol.Value{Type: protocol.Error, Str: "ERR unknown command '" + name + "'"}
	}

    // 如果在事务中，且当前命令不是控制命令（EXEC, DISCARD, MULTI, WATCH），则入队
	if ctx.Conn != nil && ctx.Conn.InMultiState && name != "EXEC" && name != "DISCARD" && name != "MULTI" && name != "WATCH" {
        // 参数校验 (Arity Check) 仍然需要在入队时做
        cmdArgCount := len(ctx.Args) + 1
        if cmd.Arity > 0 && cmdArgCount != cmd.Arity {
             return protocol.Value{Type: protocol.Error, Str: "ERR wrong number of arguments..."}
        }

		if cmd.Arity < 0 && cmdArgCount < -cmd.Arity {
			return protocol.Value{Type: protocol.Error, Str: "ERR wrong number of arguments for '" + strings.ToLower(name) + "' command"}
		}

		ctx.Conn.QueuedCmds = append(ctx.Conn.QueuedCmds, core.QueuedCmd{
			Name: name,
			Args: ctx.Args,
		})
		return protocol.Value{Type: protocol.SimpleString, Str: "QUEUED"}
	}

	return cmd.Executor(ctx)
}