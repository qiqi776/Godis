package commands

import (
	"errors"
	"godis/internal/core"
	"godis/internal/datastruct/sortedset"
	"godis/pkg/protocol"
	"godis/pkg/utils"
	"strconv"
	"strings"
)

// ZAdd 将一个或多个 member 元素及其 score 值加入到有序集 key 当中
func ZAdd(ctx *core.Context) protocol.Value {
	if len(ctx.Args) < 3 || len(ctx.Args)%2 != 1 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'zadd' command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	size := (len(ctx.Args) - 1) / 2
	// 解析 Score 和 Member
	elements := make([]*sortedset.Element, size)
	for i := 0; i < size; i++ {
		scoreStr := string(ctx.Args[1+i*2].Bulk)
		member := string(ctx.Args[2+i*2].Bulk)
		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			return protocol.Value{
				Type: protocol.Error,
				Str:  "ERR value is not a valid float",
			}
		}
		elements[i] = &sortedset.Element{Member: member, Score: score}
	}

	// 获取或创建 SortedSet
	zset, err := getOrCreateZSet(ctx.DB, ctx.Conn.SelectedDB, key)
	if err != nil {
		return protocol.Value{
			Type: protocol.Error,
			Str:  err.Error(),
		}
	}
	addedCount := 0
	for _, e := range elements {
		if zset.Add(e.Member, e.Score) {
			addedCount++
		}
	}
	ctx.DB.Set(ctx.Conn.SelectedDB, key, &core.RedisObject{
		Type: core.ObjectTypeZSet,
		Ptr:  zset,
	})
	if addedCount > 0 || size > 0 {
		writeAof(ctx, "ZADD")
	}
	return protocol.Value{Type: protocol.Integer, Num: int64(addedCount)}
}

// UndoZAdd 回滚 ZADD
func UndoZAdd(ctx *core.Context) []CmdLine {
	if len(ctx.Args) < 3 || len(ctx.Args)%2 != 1 {
		return nil
	}

	key := string(ctx.Args[0].Bulk)
	// [Fix] Pass SelectedDB
	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return []CmdLine{utils.ToCmdLine2("DEL", key)}
	}
	if obj.Type != core.ObjectTypeZSet {
		return nil
	}

	zset := obj.Ptr.(*sortedset.SortedSet)
	cmds := make([]CmdLine, 0)
	size := (len(ctx.Args) - 1) / 2

	for i := 0; i < size; i++ {
		member := string(ctx.Args[2+i*2].Bulk)
		elem, ok := zset.Get(member)
		if ok {
			oldScoreStr := strconv.FormatFloat(elem.Score, 'f', -1, 64)
			cmds = append(cmds, utils.ToCmdLine3("ZADD", []byte(key), []byte(oldScoreStr), []byte(member)))
		} else {
			cmds = append(cmds, utils.ToCmdLine3("ZREM", []byte(key), []byte(member)))
		}
	}
	return cmds
}

// ZRem 移除有序集 key 中的一个或多个成员
func ZRem(ctx *core.Context) protocol.Value {
	if len(ctx.Args) < 2 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'zrem' command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	members := ctx.Args[1:]

	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.Integer, Num: 0}
	}
	if obj.Type != core.ObjectTypeZSet {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}
	zset := obj.Ptr.(*sortedset.SortedSet)
	deleted := 0
	for _, m := range members {
		if zset.Remove(string(m.Bulk)) {
			deleted++
		}
	}
	if deleted > 0 {
		writeAof(ctx, "ZREM")
	}
	return protocol.Value{Type: protocol.Integer, Num: int64(deleted)}
}

// UndoZRem 回滚 ZREM -> 将删除的元素 ZADD 回去
func UndoZRem(ctx *core.Context) []CmdLine {
	key := string(ctx.Args[0].Bulk)
	members := ctx.Args[1:]

	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists || obj.Type != core.ObjectTypeZSet {
		return nil
	}
	zset := obj.Ptr.(*sortedset.SortedSet)
	cmds := make([]CmdLine, 0)
	for _, m := range members {
		member := string(m.Bulk)
		elem, ok := zset.Get(member)
		if ok {
			scoreStr := strconv.FormatFloat(elem.Score, 'f', -1, 64)
			cmds = append(cmds, utils.ToCmdLine3("ZADD", []byte(key), []byte(scoreStr), []byte(member)))
		}
	}
	return cmds
}

// ZScore 返回有序集中，成员 member 的 score 值
func ZScore(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 2 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'zscore' command",
		}
	}
	key := string(ctx.Args[0].Bulk)
	member := string(ctx.Args[1].Bulk)

	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.BulkString, Bulk: nil}
	}
	if obj.Type != core.ObjectTypeZSet {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}

	zset := obj.Ptr.(*sortedset.SortedSet)
	elem, ok := zset.Get(member)
	if !ok {
		return protocol.Value{Type: protocol.BulkString, Bulk: nil}
	}

	scoreStr := strconv.FormatFloat(elem.Score, 'f', -1, 64)
	return protocol.Value{Type: protocol.BulkString, Bulk: []byte(scoreStr)}
}

// ZCard 返回有序集 key 的基数
func ZCard(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 1 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'zcard' command",
		}
	}
	key := string(ctx.Args[0].Bulk)
	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.Integer, Num: 0}
	}
	if obj.Type != core.ObjectTypeZSet {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}

	zset := obj.Ptr.(*sortedset.SortedSet)
	return protocol.Value{Type: protocol.Integer, Num: zset.Len()}
}

// ZRANK / ZREVRANK
func ZRank(ctx *core.Context) protocol.Value {
	return zRankGeneric(ctx, false)
}

func ZRevRank(ctx *core.Context) protocol.Value {
	return zRankGeneric(ctx, true)
}

func zRankGeneric(ctx *core.Context, desc bool) protocol.Value {
	if len(ctx.Args) != 2 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	member := string(ctx.Args[1].Bulk)

	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.BulkString, Bulk: nil}
	}
	if obj.Type != core.ObjectTypeZSet {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}

	zset := obj.Ptr.(*sortedset.SortedSet)
	rank := zset.GetRank(member, desc)
	if rank < 0 {
		return protocol.Value{Type: protocol.BulkString, Bulk: nil}
	}
	return protocol.Value{Type: protocol.Integer, Num: rank}
}

// ZRANGE / ZREVRANGE
func ZRange(ctx *core.Context) protocol.Value {
	return zRangeGeneric(ctx, false)
}

func ZRevRange(ctx *core.Context) protocol.Value {
	return zRangeGeneric(ctx, true)
}

func zRangeGeneric(ctx *core.Context, desc bool) protocol.Value {
	if len(ctx.Args) < 3 || len(ctx.Args) > 4 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	startStr := string(ctx.Args[1].Bulk)
	stopStr := string(ctx.Args[2].Bulk)
	withScores := false

	if len(ctx.Args) == 4 {
		if strings.ToUpper(string(ctx.Args[3].Bulk)) == "WITHSCORES" {
			withScores = true
		} else {
			return protocol.Value{
				Type: protocol.Error,
				Str:  "ERR syntax error",
			}
		}
	}

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
	if obj.Type != core.ObjectTypeZSet {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}

	zset := obj.Ptr.(*sortedset.SortedSet)
	size := zset.Len()
	idxStart, idxEnd := utils.ConvertRange(start, stop, size)
	if idxStart == -1 {
		return protocol.Value{Type: protocol.Array, Array: []protocol.Value{}}
	}

	slice := zset.RangeByRank(int64(idxStart), int64(idxEnd-1), desc)
	res := make([]protocol.Value, 0, len(slice))
	for _, e := range slice {
		res = append(res, protocol.Value{Type: protocol.BulkString, Bulk: []byte(e.Member)})
		if withScores {
			scoreStr := strconv.FormatFloat(e.Score, 'f', -1, 64)
			res = append(res, protocol.Value{Type: protocol.BulkString, Bulk: []byte(scoreStr)})
		}
	}
	return protocol.Value{Type: protocol.Array, Array: res}
}

// Helpers
func getOrCreateZSet(db core.KVStorage, dbIndex int, key string) (*sortedset.SortedSet, error) {
	obj, exists := db.Get(dbIndex, key)
	if !exists {
		return sortedset.Make(), nil
	}
	if obj.Type != core.ObjectTypeZSet {
		return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return obj.Ptr.(*sortedset.SortedSet), nil
}