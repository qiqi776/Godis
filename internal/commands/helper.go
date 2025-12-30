package commands

import (
	"godis/internal/core"
	"godis/pkg/protocol"
)

// writeAof 是一个通用函数，用于将当前命令上下文写入 AOF
func writeAof(ctx *core.Context, cmdName string) {
	if aofEngine := ctx.DB.GetAof(); aofEngine != nil {
		cmdArgs := make([]protocol.Value, len(ctx.Args)+1)
		cmdArgs[0] = protocol.Value{Type: protocol.BulkString, Bulk: []byte(cmdName)}
		copy(cmdArgs[1:], ctx.Args)

		cmdLine := protocol.Value{
			Type:  protocol.Array,
			Array: cmdArgs,
		}
		aofEngine.Write(protocol.ToRespBytes(cmdLine))
	}
}
