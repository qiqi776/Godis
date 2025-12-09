package commands

import (
	"godis/internal/core"
	"godis/pkg/protocol"
	"strings"
)

func Set(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 2 {
		return protocol.Value{Type: protocol.Error, Str: "ERR wrong number of arguments for 'set' command"}
	}

	key := string(ctx.Args[0].Bulk)
	val := ctx.Args[1].Bulk

	// 1. 写入内存
	ctx.DB.Set(key, val)

	// 2. AOF 持久化
	if aofEngine := ctx.DB.GetAof(); aofEngine != nil {
		cmdLine := protocol.Value{
			Type: protocol.Array,
			Array: []protocol.Value{
				{Type: protocol.BulkString, Bulk: []byte("SET")},
				ctx.Args[0], // Key
				ctx.Args[1], // Val
			},
		}
		// 调用底部的辅助函数
		payload := toRespBytes(cmdLine) 
		aofEngine.Write(payload)
	}

	return protocol.Value{Type: protocol.SimpleString, Str: "OK"}
}

func Get(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 1 {
		return protocol.Value{Type: protocol.Error, Str: "ERR wrong number of arguments for 'get' command"}
	}

	key := string(ctx.Args[0].Bulk)
	val, ok := ctx.DB.Get(key)

	if !ok {
		return protocol.Value{Type: protocol.BulkString, Bulk: nil}
	}

	return protocol.Value{Type: protocol.BulkString, Bulk: val}
}

// 辅助函数
func toRespBytes(v protocol.Value) []byte {
	var sb strings.Builder
	sb.WriteString(protocol.MakeArrayHeader(len(v.Array)))
	for _, item := range v.Array {
		sb.WriteString(protocol.MakeBulkString(string(item.Bulk)))
	}
	return []byte(sb.String())
}