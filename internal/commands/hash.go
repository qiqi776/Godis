package commands

import (
	"errors"
	"godis/internal/core"
	"godis/internal/datastruct/dict"
	"godis/pkg/protocol"
	"godis/pkg/utils"
)

// HSet 将哈希表 key 中的字段 field 的值设为 value
func HSet(ctx *core.Context) protocol.Value {
	if len(ctx.Args) < 3 || len(ctx.Args)%2 != 1 {
		return protocol.Value{
			Type: protocol.Error, 
			Str: "ERR wrong number of arguments for 'hset' command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	size := (len(ctx.Args) - 1) / 2

	hashObj, err := getOrCreateHash(ctx.DB, ctx.Conn.SelectedDB, key)
	if err != nil {
		return protocol.Value{Type: protocol.Error, Str: err.Error()}
	}

	addedCount := 0
	for i := 0; i < size; i++ {
		field := string(ctx.Args[1+i*2].Bulk)
		value := ctx.Args[2+i*2].Bulk
		result := hashObj.Put(field, value)
		addedCount += result
	}
	ctx.DB.Set(ctx.Conn.SelectedDB, key, &core.RedisObject{
		Type: core.ObjectTypeHash,
		Ptr:  hashObj,
	})
	writeAof(ctx, "HSET")
	return protocol.Value{Type: protocol.Integer, Num: int64(addedCount)}
}

// UndoHSet 回滚 HSET
func UndoHSet(ctx *core.Context) []CmdLine {
	if len(ctx.Args) < 3 || len(ctx.Args)%2 != 1 {
		return nil
	}
	key := string(ctx.Args[0].Bulk)
	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return []CmdLine{utils.ToCmdLine2("DEL", key)}
	}
	if obj.Type != core.ObjectTypeHash {
		return nil
	}
	hashObj := obj.Ptr.(dict.Dict)
	cmds := make([]CmdLine, 0)
	size := (len(ctx.Args) - 1) / 2
	for i := 0; i < size; i++ {
		field := string(ctx.Args[1+i*2].Bulk)
		oldVal, exists := hashObj.Get(field)
		if exists {
			cmds = append(cmds, utils.ToCmdLine3("HSET", []byte(key), []byte(field), oldVal.([]byte)))
		} else {
			cmds = append(cmds, utils.ToCmdLine3("HDEL", []byte(key), []byte(field)))
		}
	}
	return cmds
}

// HGet 获取存储在哈希表中指定字段的值
func HGet(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 2 {
		return protocol.Value{
			Type: protocol.Error, 
			Str: "ERR wrong number of arguments for 'hget' command",
		}
	}
	key := string(ctx.Args[0].Bulk)
	field := string(ctx.Args[1].Bulk)

	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.BulkString, Bulk: nil}
	}
	if obj.Type != core.ObjectTypeHash {
		return protocol.Value{
			Type: protocol.Error, 
			Str: "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}

	hashObj := obj.Ptr.(dict.Dict)
	val, exists := hashObj.Get(field)
	if !exists {
		return protocol.Value{Type: protocol.BulkString, Bulk: nil}
	}
	return protocol.Value{Type: protocol.BulkString, Bulk: val.([]byte)}
}

// HGetAll 获取在哈希表中指定 key 的所有字段和值
func HGetAll(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 1 {
		return protocol.Value{
			Type: protocol.Error, 
			Str: "ERR wrong number of arguments for 'hgetall' command",
		}
	}
	key := string(ctx.Args[0].Bulk)
	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.Array, Array: []protocol.Value{}}
	}
	if obj.Type != core.ObjectTypeHash {
		return protocol.Value{
			Type: protocol.Error, 
			Str: "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}
	hashObj := obj.Ptr.(dict.Dict)
	res := make([]protocol.Value, 0, hashObj.Len()*2)

	hashObj.ForEach(func(key string, val interface{}) bool {
		res = append(res, protocol.Value{Type: protocol.BulkString, Bulk: []byte(key)})
		res = append(res, protocol.Value{Type: protocol.BulkString, Bulk: val.([]byte)})
		return true
	})
	return protocol.Value{Type: protocol.Array, Array: res}
}

// HDel 删除一个或多个哈希表字段
func HDel(ctx *core.Context) protocol.Value {
	if len(ctx.Args) < 2 {
		return protocol.Value{
			Type: protocol.Error, 
			Str: "ERR wrong number of arguments for 'hdel' command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	fields := ctx.Args[1:]

	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.Integer, Num: 0}
	}
	if obj.Type != core.ObjectTypeHash {
		return protocol.Value{
			Type: protocol.Error, 
			Str: "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}

	hashObj := obj.Ptr.(dict.Dict)
	deleted := 0
	for _, f := range fields {
		_, result := hashObj.Remove(string(f.Bulk))
		deleted += result
	}
	if hashObj.Len() == 0 {
		ctx.DB.Remove(ctx.Conn.SelectedDB, key)
	}
	if deleted > 0 {
		writeAof(ctx, "HDEL")
	}
	return protocol.Value{Type: protocol.Integer, Num: int64(deleted)}
}

// UndoHDel 回滚 HDEL
func UndoHDel(ctx *core.Context) []CmdLine {
	if len(ctx.Args) < 2 {
		return nil
	}

	key := string(ctx.Args[0].Bulk)
	fields := ctx.Args[1:]

	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists || obj.Type != core.ObjectTypeHash {
		return nil
	}

	hashObj := obj.Ptr.(dict.Dict)
	cmds := make([]CmdLine, 0)

	for _, f := range fields {
		field := string(f.Bulk)
		val, exists := hashObj.Get(field)
		if exists {
			cmds = append(cmds, utils.ToCmdLine3("HSET", []byte(key), []byte(field), val.([]byte)))
		}
	}
	return cmds
}

// HExists 查看哈希表 key 中，指定的字段是否存在
func HExists(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 2 {
		return protocol.Value{
			Type: protocol.Error,
			Str: "ERR wrong number of arguments for 'hexists' command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	field := string(ctx.Args[1].Bulk)
	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.Integer, Num: 0}
	}
	if obj.Type != core.ObjectTypeHash {
		return protocol.Value{
			Type: protocol.Error, 
			Str: "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}

	hashObj := obj.Ptr.(dict.Dict)
	_, exists = hashObj.Get(field)
	if exists {
		return protocol.Value{Type: protocol.Integer, Num: 1}
	}
	return protocol.Value{Type: protocol.Integer, Num: 0}
}

// HLen 获取哈希表中字段的数量
func HLen(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 1 {
		return protocol.Value{
			Type: protocol.Error, 
			Str: "ERR wrong number of arguments for 'hlen' command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.Integer, Num: 0}
	}
	if obj.Type != core.ObjectTypeHash {
		return protocol.Value{
			Type: protocol.Error, 
			Str: "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}

	hashObj := obj.Ptr.(dict.Dict)
	return protocol.Value{Type: protocol.Integer, Num: int64(hashObj.Len())}
}

// Helpers
func getOrCreateHash(db core.KVStorage, dbIndex int, key string) (dict.Dict, error) {
	obj, exists := db.Get(dbIndex, key)
	if !exists {
		return dict.MakeSimple(), nil
	}
	if obj.Type != core.ObjectTypeHash {
		return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return obj.Ptr.(dict.Dict), nil
}