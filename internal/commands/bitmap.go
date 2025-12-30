package commands

import (
	"godis/internal/core"
	"godis/internal/datastruct/bitmap"
	"godis/pkg/protocol"
	"godis/pkg/utils"
	"math/bits"
	"strconv"
)

// SetBit 设置或清除指定偏移量上的位
func SetBit(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 3 {
		return protocol.Value{
			Type: protocol.Error, 
			Str: "ERR wrong number of arguments for 'setbit' command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	offsetStr := string(ctx.Args[1].Bulk)
	valStr := string(ctx.Args[2].Bulk)

	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil || offset < 0 {
		return protocol.Value{
			Type: protocol.Error, 
			Str: "ERR bit offset is not an integer or out of range",
		}
	}

	valInt, err := strconv.Atoi(valStr)
	if err != nil || (valInt != 0 && valInt != 1) {
		return protocol.Value{
			Type: protocol.Error, 
			Str: "ERR bit is not an integer or out of range",
		}
	}
	val := byte(valInt)
	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if exists && obj.Type != core.ObjectTypeString {
		return protocol.Value{
			Type: protocol.Error, 
			Str: "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}

	var bm *bitmap.BitMap
	if !exists {
		bm = bitmap.Make()
	} else {
		bytes := obj.Ptr.([]byte)
		bm = bitmap.FromBytes(bytes)
	}

	oldBit := bm.GetBit(offset)
	bm.SetBit(offset, val)
	ctx.DB.Set(ctx.Conn.SelectedDB, key, &core.RedisObject{
		Type: core.ObjectTypeString,
		Ptr:  bm.ToBytes(),
	})

	writeAof(ctx, "SETBIT")

	return protocol.Value{Type: protocol.Integer, Num: int64(oldBit)}
}

// UndoSetBit 回滚 SETBIT -> 将该位设置回旧值
func UndoSetBit(ctx *core.Context) []CmdLine {
	key := string(ctx.Args[0].Bulk)
	offsetStr := string(ctx.Args[1].Bulk)

	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		return nil
	}

	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return []CmdLine{utils.ToCmdLine2("DEL", key)}
	}
	if obj.Type != core.ObjectTypeString {
		return nil
	}

	bytes := obj.Ptr.([]byte)
	bm := bitmap.FromBytes(bytes)
	oldVal := bm.GetBit(offset)
	return []CmdLine{
		utils.ToCmdLine2("SETBIT", key, offsetStr, strconv.Itoa(int(oldVal))),
	}
}

// GetBit 对 key 所储存的字符串值，获取指定偏移量上的位
func GetBit(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 2 {
		return protocol.Value{
			Type: protocol.Error, 
			Str: "ERR wrong number of arguments for 'getbit' command",
		}
	}

	key := string(ctx.Args[0].Bulk)
	offsetStr := string(ctx.Args[1].Bulk)

	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil || offset < 0 {
		return protocol.Value{
			Type: protocol.Error, 
			Str: "ERR bit offset is not an integer or out of range",
		}
	}
	obj, exists := ctx.DB.Get(ctx.Conn.SelectedDB, key)
	if !exists {
		return protocol.Value{Type: protocol.Integer, Num: 0}
	}
	if obj.Type != core.ObjectTypeString {
		return protocol.Value{
			Type: protocol.Error, 
			Str: "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}
	bytes := obj.Ptr.([]byte)
	bm := bitmap.FromBytes(bytes)
	return protocol.Value{Type: protocol.Integer, Num: int64(bm.GetBit(offset))}
}

// BitCount 统计字符串被设置为 1 的 bit 数
func BitCount(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 1 && len(ctx.Args) != 3 {
		return protocol.Value{
			Type: protocol.Error, 
			Str: "ERR wrong number of arguments for 'bitcount' command",
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
			Str: "WRONGTYPE Operation against a key holding the wrong kind of value",
		}
	}

	bytes := obj.Ptr.([]byte)
	bm := bitmap.FromBytes(bytes)
	size := int64(len(bytes))

	start := int64(0)
	end := int64(-1)

	if len(ctx.Args) == 3 {
		var err error
		start, err = strconv.ParseInt(string(ctx.Args[1].Bulk), 10, 64)
		if err != nil {
			return protocol.Value{
				Type: protocol.Error, 
				Str: "ERR value is not an integer or out of range",
			}
		}
		end, err = strconv.ParseInt(string(ctx.Args[2].Bulk), 10, 64)
		if err != nil {
			return protocol.Value{
				Type: protocol.Error, 
				Str: "ERR value is not an integer or out of range",
			}
		}
	}

	idxStart, idxEnd := utils.ConvertRange(start, end, size)
	if idxStart == -1 {
		return protocol.Value{Type: protocol.Integer, Num: 0}
	}
	count := 0
	bm.ForEachByte(idxStart, idxEnd, func(offset int, val byte) bool {
		count += bits.OnesCount8(val)
		return true
	})

	return protocol.Value{Type: protocol.Integer, Num: int64(count)}
}