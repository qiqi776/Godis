package commands

import (
	"godis/internal/core"
	"godis/pkg/protocol"
	"godis/pkg/utils"
)

// Del 删除指定的 key
func Del(ctx *core.Context) protocol.Value {
	keys := ctx.Args
	deleted := 0
	for _, keyArg := range keys {
		key := string(keyArg.Bulk)
		deleted += ctx.DB.Remove(ctx.Conn.SelectedDB, key)
	}
	if deleted > 0 {
		writeAof(ctx, "DEL")
	}
	return protocol.Value{Type: protocol.Integer, Num: int64(deleted)}
}

// UndoDel 回滚删除 -> 恢复原来的值
func UndoDel(ctx *core.Context) []CmdLine {
	cmds := make([]CmdLine, 0)
	for _, arg := range ctx.Args {
		key := string(arg.Bulk)
		obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
		if exists {
			// 目前仅演示 String 类型的恢复
			if obj.Type == core.ObjectTypeString {
				cmds = append(cmds, utils.ToCmdLine3("SET", arg.Bulk, obj.Ptr.([]byte)))
			}
			// TODO: 支持其他数据结构的回滚
		}
	}
	return cmds
}

// Exists 检查 key 是否存在
func Exists(ctx *core.Context) protocol.Value {
	count := int64(0)
	for _, arg := range ctx.Args {
		key := string(arg.Bulk)
		if _, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key); exists {
			count++
		}
	}
	return protocol.Value{Type: protocol.Integer, Num: count}
}

// Type 返回 key 存储的值的类型
func Type(ctx *core.Context) protocol.Value {
	key := string(ctx.Args[0].Bulk)
	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.SimpleString, Str: "none"}
	}

	switch obj.Type {
	case core.ObjectTypeString:
		return protocol.Value{Type: protocol.SimpleString, Str: "string"}
	case core.ObjectTypeList:
		return protocol.Value{Type: protocol.SimpleString, Str: "list"}
	case core.ObjectTypeHash:
		return protocol.Value{Type: protocol.SimpleString, Str: "hash"}
	case core.ObjectTypeSet:
		return protocol.Value{Type: protocol.SimpleString, Str: "set"}
	case core.ObjectTypeZSet:
		return protocol.Value{Type: protocol.SimpleString, Str: "zset"}
	}
	return protocol.Value{Type: protocol.SimpleString, Str: "unknown"}
}

// KEYS
func Keys(ctx *core.Context) protocol.Value {
    if len(ctx.Args) != 1 {
        return protocol.Value{Type: protocol.Error, 
			Str: "ERR wrong number of arguments for 'keys' command",
		}
    }
    pattern := string(ctx.Args[0].Bulk)
    if pattern != "*" {
        return protocol.Value{
			Type: protocol.Error, 
			Str: "ERR only * pattern supported currently",
		}
    }
    
    keys := ctx.DB.Keys(ctx.Conn.SelectedDB, pattern)
    res := make([]protocol.Value, len(keys))
    for i, k := range keys {
        res[i] = protocol.Value{Type: protocol.BulkString, Bulk: []byte(k)}
    }
    return protocol.Value{Type: protocol.Array, Array: res}
}

// FLUSHDB
func FlushDB(ctx *core.Context) protocol.Value {
    ctx.DB.FlushDB(ctx.Conn.SelectedDB)
    writeAof(ctx, "FLUSHDB")
    return protocol.Value{Type: protocol.SimpleString, Str: "OK"}
}

// FLUSHALL
func FlushAll(ctx *core.Context) protocol.Value {
    ctx.DB.FlushAll()
    writeAof(ctx, "FLUSHALL")
    return protocol.Value{Type: protocol.SimpleString, Str: "OK"}
}

// DBSIZE
func DBSize(ctx *core.Context) protocol.Value {
    size := ctx.DB.KeyCount(ctx.Conn.SelectedDB)
    return protocol.Value{Type: protocol.Integer, Num: int64(size)}
}