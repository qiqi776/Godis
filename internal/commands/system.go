package commands

import (
	"godis/internal/core"
	"godis/pkg/protocol"
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
	keyCount := ctx.DB.KeyCount()
	
	infoStr := stats.GetInfo(keyCount)
	return protocol.Value{Type: protocol.BulkString, Bulk: []byte(infoStr)}
}