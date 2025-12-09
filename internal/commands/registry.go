package commands

import (
	"godis/internal/core"
	"godis/pkg/protocol"
	"strings"
)

// 是全局命令注册表
var Commands = make(map[string]core.CommandFunc)

// 注册一个新命令
func Register(name string, cmd core.CommandFunc) {
	Commands[strings.ToUpper(name)] = cmd
}

// 获取命令处理函数
func Lookup(name string) (core.CommandFunc, bool) {
	cmd, ok := Commands[strings.ToUpper(name)]
	return cmd, ok
}

// 初始化所有命令
func Init() {
	Register("SET", Set)
	Register("GET", Get)

	Register("PING", Ping)
	Register("INFO", Info)
}

// 统一的执行入口
func Execute(name string, ctx *core.Context) protocol.Value {
	cmd, ok := Lookup(name)
	if !ok {
		return protocol.Value{Type: protocol.Error, Str: "ERR unknown command '" + name + "'"}
	}
	return cmd(ctx)
}