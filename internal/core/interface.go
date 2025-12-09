package core

import (
	"godis/internal/aof"
	"godis/internal/database"
	"godis/pkg/protocol"
)

// 命令执行时的上下文
type Context struct {
	Args []protocol.Value // 命令参数
	DB   KVStorage        // 数据库接口
}

// 每个Redis命令的处理函数签名
type CommandFunc func(ctx *Context) protocol.Value

// 定义底层存储引擎必须实现的方法
type KVStorage interface {
	// 基础读写
	Get(key string) ([]byte, bool)
	Set(key string, value []byte)
	
	// 统计相关
	GetStats() *database.Stats
	KeyCount() int

	// 持久化相关
	GetAof() *aof.Aof
	SetAof(aof *aof.Aof)
}