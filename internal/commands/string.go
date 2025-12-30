package commands

import (
	"godis/internal/core"
	"godis/pkg/protocol"
	"godis/pkg/utils"
)

// Set 将字符串值 value 关联到 key
func Set(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 2 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'set' command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	val := ctx.Args[1].Bulk
	entity := &core.RedisObject{
		Type: core.ObjectTypeString,
		Ptr:  val,
	}
	ctx.DB.Set(ctx.Conn.SelectedDB, key, entity)
	writeAof(ctx, "SET")
	return protocol.Value{Type: protocol.SimpleString, Str: "OK"}
}

// UndoSet 生成 SET 的回滚命令
func UndoSet(ctx *core.Context) []CmdLine {
	key := string(ctx.Args[0].Bulk)
	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)

	if !exists {
		return []CmdLine{utils.ToCmdLine2("DEL", key)}
	}
	if obj.Type == core.ObjectTypeString {
		oldVal := obj.Ptr.([]byte)
		return []CmdLine{utils.ToCmdLine3("SET", []byte(key), oldVal)}
	}
	return nil
}

// Get 返回 key 所关联的字符串值
func Get(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 1 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'get' command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.BulkString, Bulk: nil}
	}
	if obj.Type != core.ObjectTypeString {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}

	val := obj.Ptr.([]byte)
	return protocol.Value{Type: protocol.BulkString, Bulk: val}
}

// SETNX
func SetNX(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 2 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'setnx' command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	val := ctx.Args[1].Bulk
	_, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if exists {
		return protocol.Value{Type: protocol.Integer, Num: 0}
	}
	entity := &core.RedisObject{
		Type: core.ObjectTypeString,
		Ptr:  val,
	}

	ctx.DB.Set(ctx.Conn.SelectedDB, key, entity)
	writeAof(ctx, "SETNX")
	return protocol.Value{Type: protocol.Integer, Num: 1}
}

// UndoSetNX 回滚 SETNX
func UndoSetNX(ctx *core.Context) []CmdLine {
	key := string(ctx.Args[0].Bulk)
	_, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)

	if !exists {
		return []CmdLine{utils.ToCmdLine2("DEL", key)}
	}
	return nil
}

// GETSET
func GetSet(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 2 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'getset' command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	newVal := ctx.Args[1].Bulk

	oldObj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if exists && oldObj.Type != core.ObjectTypeString {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}

	entity := &core.RedisObject{
		Type: core.ObjectTypeString,
		Ptr:  newVal,
	}
	ctx.DB.Set(ctx.Conn.SelectedDB, key, entity)
	writeAof(ctx, "GETSET")
	if !exists {
		return protocol.Value{Type: protocol.BulkString, Bulk: nil}
	}
	return protocol.Value{Type: protocol.BulkString, Bulk: oldObj.Ptr.([]byte)}
}

// UndoGetSet 回滚 GETSET
func UndoGetSet(ctx *core.Context) []CmdLine {
	key := string(ctx.Args[0].Bulk)
	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return []CmdLine{utils.ToCmdLine2("DEL", key)}
	}
	if obj.Type == core.ObjectTypeString {
		return []CmdLine{utils.ToCmdLine3("SET", []byte(key), obj.Ptr.([]byte))}
	}
	return nil
}

// STRLEN
func StrLen(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 1 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'strlen' command",
		}
	}
	key := string(ctx.Args[0].Bulk)
	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.Integer, Num: 0}
	}
	if obj.Type != core.ObjectTypeString {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}

	val := obj.Ptr.([]byte)
	return protocol.Value{Type: protocol.Integer, Num: int64(len(val))}
}

// MSET
func MSet(ctx *core.Context) protocol.Value {
	if len(ctx.Args) < 2 || len(ctx.Args)%2 != 0 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'mset' command",
		}
	}

	size := len(ctx.Args) / 2
	for i := 0; i < size; i++ {
		key := string(ctx.Args[2*i].Bulk)
		val := ctx.Args[2*i+1].Bulk
		entity := &core.RedisObject{
			Type: core.ObjectTypeString,
			Ptr:  val,
		}
		ctx.DB.Set(ctx.Conn.SelectedDB, key, entity)
	}
	writeAof(ctx, "MSET")
	return protocol.Value{Type: protocol.SimpleString, Str: "OK"}
}

// UndoMSet 回滚 MSET
func UndoMSet(ctx *core.Context) []CmdLine {
	size := len(ctx.Args) / 2
	cmds := make([]CmdLine, 0, size)

	for i := 0; i < size; i++ {
		key := string(ctx.Args[2*i].Bulk)
		obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
		if exists {
			if obj.Type == core.ObjectTypeString {
				cmds = append(cmds, utils.ToCmdLine3("SET", []byte(key), obj.Ptr.([]byte)))
			} else {
				cmds = append(cmds, utils.ToCmdLine2("DEL", key))
			}
		} else {
			cmds = append(cmds, utils.ToCmdLine2("DEL", key))
		}
	}
	return cmds
}

// MGET
func MGet(ctx *core.Context) protocol.Value {
	if len(ctx.Args) < 1 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'mget' command",
		}
	}
	res := make([]protocol.Value, len(ctx.Args))
	for i, arg := range ctx.Args {
		key := string(arg.Bulk)
		obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)

		if !exists || obj.Type != core.ObjectTypeString {
			res[i] = protocol.Value{Type: protocol.BulkString, Bulk: nil}
		} else {
			res[i] = protocol.Value{Type: protocol.BulkString, Bulk: obj.Ptr.([]byte)}
		}
	}
	return protocol.Value{Type: protocol.Array, Array: res}
}