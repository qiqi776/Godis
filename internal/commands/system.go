package commands

import (
	"godis/internal/core"
	"godis/pkg/protocol"
	"strconv"
)

func Ping(ctx *core.Context) protocol.Value {
	if len(ctx.Args) == 0 {
		return protocol.Value{Type: protocol.SimpleString, Str: "PONG"}
	}
	return protocol.Value{Type: protocol.BulkString, Bulk: ctx.Args[0].Bulk}
}

func Info(ctx *core.Context) protocol.Value {
	// 调用 DB 接口获取统计信息
	stats := ctx.DB.GetStats()
	keyCount := ctx.DB.KeyCount(ctx.Conn.SelectedDB)

	infoStr := stats.GetInfo(keyCount)
	return protocol.Value{Type: protocol.BulkString, Bulk: []byte(infoStr)}
}

func Select(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 1 {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'select' command",
		}
	}

	indexStr := string(ctx.Args[0].Bulk)
	index, err := strconv.Atoi(indexStr)
	if err != nil {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR value is not an integer or out of range",
		}
	}

	if !ctx.DB.IsValidDBIndex(index) {
		return protocol.Value{
			Type: protocol.Error,
			Str:  "ERR DB index is out of range",
		}
	}

	ctx.Conn.SelectedDB = index
	return protocol.Value{Type: protocol.SimpleString, Str: "OK"}
}