package commands

import (
	"errors"
	"godis/internal/core"
	"godis/internal/datastruct/list"
	"godis/pkg/protocol"
	"godis/pkg/utils"
	"strconv"
)

// LPush 将一个或多个值插入到列表头部
func LPush(ctx *core.Context) protocol.Value {
	if len(ctx.Args) < 2 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'lpush' command",
		}
	}
	key := string(ctx.Args[0].Bulk)
	values := ctx.Args[1:]
	listObj, err := getOrCreateList(ctx.DB, ctx.Conn.SelectedDB, key)
	if err != nil {
		return protocol.Value{
			Type: protocol.Error,
			Str:  err.Error(),
		}
	}
	for _, v := range values {
		listObj.Insert(0, v.Bulk)
	}
	ctx.DB.Set(ctx.Conn.SelectedDB, key, &core.RedisObject{
		Type: core.ObjectTypeList,
		Ptr:  listObj,
	})
	writeAof(ctx, "LPUSH")
	return protocol.Value{Type: protocol.Integer, Num: int64(listObj.Len())}
}

// RPush 将一个或多个值插入到列表尾部
func RPush(ctx *core.Context) protocol.Value {
	if len(ctx.Args) < 2 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'rpush' command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	values := ctx.Args[1:]

	listObj, err := getOrCreateList(ctx.DB, ctx.Conn.SelectedDB, key)
	if err != nil {
		return protocol.Value{
			Type: protocol.Error,
			Str:  err.Error(),
		}
	}

	for _, v := range values {
		listObj.Add(v.Bulk)
	}

	ctx.DB.Set(ctx.Conn.SelectedDB, key, &core.RedisObject{
		Type: core.ObjectTypeList,
		Ptr:  listObj,
	})

	writeAof(ctx, "RPUSH")

	return protocol.Value{Type: protocol.Integer, Num: int64(listObj.Len())}
}

// LPop 移除并返回列表的第一个元素
func LPop(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 1 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'lpop' command",
		}
	}
	key := string(ctx.Args[0].Bulk)
	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.BulkString, Bulk: nil}
	}
	if obj.Type != core.ObjectTypeList {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}
	listObj := obj.Ptr.(*list.QuickList)
	if listObj.Len() == 0 {
		return protocol.Value{Type: protocol.BulkString, Bulk: nil}
	}
	// 执行移除 (Remove index 0)
	val := listObj.Remove(0)
	if listObj.Len() == 0 {
		ctx.DB.Remove(ctx.Conn.SelectedDB, key)
	}

	writeAof(ctx, "LPOP")

	return protocol.Value{Type: protocol.BulkString, Bulk: val.([]byte)}
}

// RPop 移除并返回列表的最后一个元素
func RPop(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 1 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'rpop' command",
		}
	}

	key := string(ctx.Args[0].Bulk)

	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.BulkString, Bulk: nil}
	}

	if obj.Type != core.ObjectTypeList {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}

	listObj := obj.Ptr.(*list.QuickList)
	if listObj.Len() == 0 {
		return protocol.Value{Type: protocol.BulkString, Bulk: nil}
	}

	val := listObj.RemoveLast()

	if listObj.Len() == 0 {
		ctx.DB.Remove(ctx.Conn.SelectedDB, key)
	}

	writeAof(ctx, "RPOP")

	return protocol.Value{Type: protocol.BulkString, Bulk: val.([]byte)}
}

// LRange 获取列表指定范围内的元素
func LRange(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 3 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'lrange' command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	startStr := string(ctx.Args[1].Bulk)
	stopStr := string(ctx.Args[2].Bulk)

	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR value is not an integer or out of range",
		}
	}
	stop, err := strconv.ParseInt(stopStr, 10, 64)
	if err != nil {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR value is not an integer or out of range",
		}
	}

	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.Array, Array: []protocol.Value{}}
	}

	if obj.Type != core.ObjectTypeList {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}

	listObj := obj.Ptr.(*list.QuickList)
	size := int64(listObj.Len())

	// ConvertRange 已经返回了 [start, end) 的左闭右开区间
	idxStart, idxEnd := utils.ConvertRange(start, stop, size)
	if idxStart == -1 {
		return protocol.Value{Type: protocol.Array, Array: []protocol.Value{}}
	}

	rawSlice := listObj.Range(idxStart, idxEnd)

	respArray := make([]protocol.Value, len(rawSlice))
	for i, v := range rawSlice {
		respArray[i] = protocol.Value{
			Type: protocol.BulkString,
			Bulk: v.([]byte),
		}
	}

	return protocol.Value{Type: protocol.Array, Array: respArray}
}

// LLen 获取列表长度
func LLen(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 1 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'llen' command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.Integer, Num: 0}
	}

	if obj.Type != core.ObjectTypeList {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}

	listObj := obj.Ptr.(*list.QuickList)
	return protocol.Value{Type: protocol.Integer, Num: int64(listObj.Len())}
}

// getOrCreateList 尝试从 DB 获取 List，如果不存在则创建一个新的
func getOrCreateList(db core.KVStorage, dbIndex int, key string) (*list.QuickList, error) {
	obj, exists := db.Get(dbIndex, key)
	if !exists {
		return list.MakeQuickList(), nil
	}
	if obj.Type != core.ObjectTypeList {
		return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	return obj.Ptr.(*list.QuickList), nil
}

// UndoLPush 等 Undo 函数逻辑 (如果之前有生成，可以保留或放在单独的文件，这里为了简洁只保留了执行逻辑)
func UndoLPush(ctx *core.Context) []CmdLine {
	key := string(ctx.Args[0].Bulk)
	count := len(ctx.Args) - 1
	cmds := make([]CmdLine, 0, count)
	for i := 0; i < count; i++ {
		cmds = append(cmds, utils.ToCmdLine2("LPOP", key))
	}
	return cmds
}

func UndoRPush(ctx *core.Context) []CmdLine {
	key := string(ctx.Args[0].Bulk)
	count := len(ctx.Args) - 1
	cmds := make([]CmdLine, 0, count)
	for i := 0; i < count; i++ {
		cmds = append(cmds, utils.ToCmdLine2("RPOP", key))
	}
	return cmds
}

func UndoLPop(ctx *core.Context) []CmdLine {
	key := string(ctx.Args[0].Bulk)
	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists || obj.Type != core.ObjectTypeList {
		return nil
	}
	listObj := obj.Ptr.(*list.QuickList)
	if listObj.Len() == 0 {
		return nil
	}
	val := listObj.Get(0).([]byte)
	return []CmdLine{utils.ToCmdLine3("LPUSH", []byte(key), val)}
}

func UndoRPop(ctx *core.Context) []CmdLine {
	key := string(ctx.Args[0].Bulk)
	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists || obj.Type != core.ObjectTypeList {
		return nil
	}
	listObj := obj.Ptr.(*list.QuickList)
	if listObj.Len() == 0 {
		return nil
	}
	val := listObj.Get(listObj.Len()-1).([]byte)
	return []CmdLine{utils.ToCmdLine3("RPUSH", []byte(key), val)}
}