package commands

import (
	"errors"
	"godis/internal/core"
	"godis/internal/datastruct/set"
	"godis/pkg/protocol"
	"godis/pkg/utils"
	"strconv"
)

// SAdd 将一个或多个成员元素加入到集合中，已经存在于集合的成员元素将被忽略
func SAdd(ctx *core.Context) protocol.Value {
	if len(ctx.Args) < 2 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'sadd' command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	members := ctx.Args[1:]

	setObj, err := getOrCreateSet(ctx.DB, ctx.Conn.SelectedDB, key)
	if err != nil {
		return protocol.Value{
			Type: protocol.Error,
			Str:  err.Error(),
		}
	}

	addedCount := 0
	for _, m := range members {
		addedCount += setObj.Add(string(m.Bulk))
	}

	ctx.DB.Set(ctx.Conn.SelectedDB, key, &core.RedisObject{
		Type: core.ObjectTypeSet,
		Ptr:  setObj,
	})
	if addedCount > 0 {
		writeAof(ctx, "SADD")
	}
	return protocol.Value{Type: protocol.Integer, Num: int64(addedCount)}
}

// UndoSAdd 回滚 SADD 操作
func UndoSAdd(ctx *core.Context) []CmdLine {
	key := string(ctx.Args[0].Bulk)
	members := ctx.Args[1:]

	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return []CmdLine{utils.ToCmdLine2("DEL", key)}
	}
	if obj.Type != core.ObjectTypeSet {
		return nil
	}

	setObj := obj.Ptr.(*set.Set)
	cmds := make([]CmdLine, 0)
	for _, m := range members {
		memberVal := string(m.Bulk)
		if !setObj.Has(memberVal) {
			cmds = append(cmds, utils.ToCmdLine3("SREM", []byte(key), m.Bulk))
		}
	}
	return cmds
}

// SRem 移除集合中的一个或多个成员元素
func SRem(ctx *core.Context) protocol.Value {
	if len(ctx.Args) < 2 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'srem' command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	members := ctx.Args[1:]

	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.Integer, Num: 0}
	}
	if obj.Type != core.ObjectTypeSet {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}

	setObj := obj.Ptr.(*set.Set)
	removedCount := 0
	for _, m := range members {
		removedCount += setObj.Remove(string(m.Bulk))
	}

	// 如果集合为空，删除 key
	if setObj.Len() == 0 {
		ctx.DB.Remove(ctx.Conn.SelectedDB, key)
	}
	if removedCount > 0 {
		writeAof(ctx, "SREM")
	}
	return protocol.Value{Type: protocol.Integer, Num: int64(removedCount)}
}

// UndoSrem 回滚 SREM 操作
func UndoSrem(ctx *core.Context) []CmdLine {
	key := string(ctx.Args[0].Bulk)
	members := ctx.Args[1:]

	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	var setObj *set.Set
	if exists {
		if obj.Type != core.ObjectTypeSet {
			return nil
		}
		setObj = obj.Ptr.(*set.Set)
	}
	cmds := make([]CmdLine, 0)
	for _, m := range members {
		memberVal := string(m.Bulk)
		if setObj != nil && setObj.Has(memberVal) {
			cmds = append(cmds, utils.ToCmdLine3("SADD", []byte(key), m.Bulk))
		}
	}
	return cmds
}

// SIsMember 判断 member 元素是否是集合的成员
func SIsMember(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 2 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'sismember' command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	member := string(ctx.Args[1].Bulk)

	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.Integer, Num: 0}
	}
	if obj.Type != core.ObjectTypeSet {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}

	setObj := obj.Ptr.(*set.Set)
	if setObj.Has(member) {
		return protocol.Value{Type: protocol.Integer, Num: 1}
	}
	return protocol.Value{Type: protocol.Integer, Num: 0}
}

// SMembers 返回集合中的所有成员
func SMembers(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 1 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'smembers' command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.Array, Array: []protocol.Value{}}
	}
	if obj.Type != core.ObjectTypeSet {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}

	setObj := obj.Ptr.(*set.Set)
	members := setObj.ToSlice()

	res := make([]protocol.Value, len(members))
	for i, v := range members {
		res[i] = protocol.Value{Type: protocol.BulkString, Bulk: []byte(v)}
	}
	return protocol.Value{Type: protocol.Array, Array: res}
}

// SCard 获取集合的成员数
func SCard(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 1 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'scard' command",
		}
	}
	key := string(ctx.Args[0].Bulk)
	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.Integer, Num: 0}
	}
	if obj.Type != core.ObjectTypeSet {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}
	setObj := obj.Ptr.(*set.Set)
	return protocol.Value{Type: protocol.Integer, Num: int64(setObj.Len())}
}

// SPop 移除并返回集合中的一个或多个随机元素
func SPop(ctx *core.Context) protocol.Value {
	if len(ctx.Args) < 1 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'spop' command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	count := 1
	if len(ctx.Args) > 1 {
		c, err := strconv.Atoi(string(ctx.Args[1].Bulk))
		if err != nil {
			return protocol.Value{
				Type: protocol.Error,
				Str:  "ERR value is not an integer or out of range",
			}
		}
		count = c
	}

	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.BulkString, Bulk: nil}
	}
	if obj.Type != core.ObjectTypeSet {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}

	setObj := obj.Ptr.(*set.Set)
	members := setObj.RandomDistinctMembers(count)
	if len(members) == 0 {
		return protocol.Value{Type: protocol.BulkString, Bulk: nil}
	}

	res := make([]protocol.Value, 0, len(members))
	for _, m := range members {
		setObj.Remove(m)
		res = append(res, protocol.Value{Type: protocol.BulkString, Bulk: []byte(m)})
	}

	if setObj.Len() == 0 {
		ctx.DB.Remove(ctx.Conn.SelectedDB, key)
	}

	writeAof(ctx, "SPOP")

	if len(ctx.Args) == 1 {
		return res[0]
	}
	return protocol.Value{Type: protocol.Array, Array: res}
}

// SRandMember 返回集合中一个或多个随机数
func SRandMember(ctx *core.Context) protocol.Value {
	if len(ctx.Args) < 1 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'srandmember' command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	count := 1
	allowDuplication := false

	if len(ctx.Args) > 1 {
		c, err := strconv.Atoi(string(ctx.Args[1].Bulk))
		if err != nil {
			return protocol.Value{
				Type: protocol.Error,
				Str:  "ERR value is not an integer or out of range",
			}
		}
		if c < 0 {
			allowDuplication = true
			count = -c
		} else {
			count = c
		}
	}

	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.BulkString, Bulk: nil}
	}
	if obj.Type != core.ObjectTypeSet {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}

	setObj := obj.Ptr.(*set.Set)
	var members []string

	if allowDuplication {
		members = setObj.RandomMembers(count)
	} else {
		members = setObj.RandomDistinctMembers(count)
	}

	if len(ctx.Args) == 1 {
		if len(members) == 0 {
			return protocol.Value{Type: protocol.BulkString, Bulk: nil}
		}
		return protocol.Value{Type: protocol.BulkString, Bulk: []byte(members[0])}
	}

	res := make([]protocol.Value, len(members))
	for i, v := range members {
		res[i] = protocol.Value{Type: protocol.BulkString, Bulk: []byte(v)}
	}
	return protocol.Value{Type: protocol.Array, Array: res}
}

// getOrCreateSet
func getOrCreateSet(db core.KVStorage, dbIndex int, key string) (*set.Set, error) {
	obj, exists := db.Get(dbIndex, key)
	if !exists {
		return set.Make(), nil
	}
	if obj.Type != core.ObjectTypeSet {
		return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return obj.Ptr.(*set.Set), nil
}