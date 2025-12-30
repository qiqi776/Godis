package commands

import (
	"godis/internal/core"
	"godis/pkg/protocol"
)

// Multi 开启事务
func Multi(ctx *core.Context) protocol.Value {
	if ctx.Conn.InMultiState {
		return protocol.Value{Type: protocol.Error, Str: "ERR MULTI calls can not be nested"}
	}
	ctx.Conn.InMultiState = true
	ctx.Conn.QueuedCmds = make([]core.QueuedCmd, 0)
	return protocol.Value{Type: protocol.SimpleString, Str: "OK"}
}

// Discard 取消事务
func Discard(ctx *core.Context) protocol.Value {
	if !ctx.Conn.InMultiState {
		return protocol.Value{Type: protocol.Error, Str: "ERR DISCARD without MULTI"}
	}
	ctx.Conn.InMultiState = false
	ctx.Conn.QueuedCmds = nil
	return protocol.Value{Type: protocol.SimpleString, Str: "OK"}
}

// Exec 执行事务
func Exec(ctx *core.Context) protocol.Value {
	if !ctx.Conn.InMultiState {
		return protocol.Value{Type: protocol.Error, Str: "ERR EXEC without MULTI"}
	}

	// 退出事务状态，防止命令执行时又入队
	ctx.Conn.InMultiState = false
	defer func() {
		ctx.Conn.QueuedCmds = nil
	}()
	if len(ctx.Conn.QueuedCmds) == 0 {
		return protocol.Value{Type: protocol.Array, Array: []protocol.Value{}}
	}
	results := make([]protocol.Value, len(ctx.Conn.QueuedCmds))
	for i, cmd := range ctx.Conn.QueuedCmds {
		subCtx := &core.Context{
			Args: cmd.Args,
			DB:   ctx.DB,
			Conn: ctx.Conn,
		}
		results[i] = Execute(cmd.Name, subCtx)
	}

	return protocol.Value{Type: protocol.Array, Array: results}
}