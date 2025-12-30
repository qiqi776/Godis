package core

import (
	"godis/internal/aof"
	"godis/pkg/protocol"
	"time"
)

type CmdLine = [][]byte

// QueuedCmd 存储事务中排队的命令
type QueuedCmd struct {
	Name string
	Args []protocol.Value
}

// Connection 代表一个客户端连接的状态
type Connection struct {
	InMultiState bool              // 是否处于事务模式
	QueuedCmds   []QueuedCmd       // 事务队列
	Watching     map[string]uint32 // WATCH 的键 (乐观锁版本号，简化版可暂时不做)
	TxErrors     []error           // 事务期间的入队错误
	SelectedDB   int               // 当前选中的数据库索引
}

// NewConnection 初始化连接
func NewConnection() *Connection {
	return &Connection{
		QueuedCmds: make([]QueuedCmd, 0),
		Watching:   make(map[string]uint32),
		SelectedDB: 0,
	}
}

type Context struct {
	Args []protocol.Value
	DB   KVStorage
	Conn *Connection
}

type CommandFunc func(ctx *Context) protocol.Value

// KVStorage 所有操作数据的方法都需要 dbIndex 参数
type KVStorage interface {
	// 数据操作
	Get(dbIndex int, key string) (*RedisObject, bool)
	Set(dbIndex int, key string, value *RedisObject)
	Remove(dbIndex int, key string) int

	// 过期时间操作
	SetExpiration(dbIndex int, key string, ddl time.Time) (bool, error)
	RmExpiration(dbIndex int, key string) (bool, error)
	GetTTL(dbIndex int, key string) (time.Duration, bool, error)

	// 统计信息
	GetStats() *Stats
	KeyCount(dbIndex int) int

	// AOF 持久化
	GetAof() *aof.Aof
	SetAof(aof *aof.Aof)

	
	IsValidDBIndex(dbIndex int) bool

    FlushDB(dbIndex int)
    FlushAll()
	Close()
    Keys(dbIndex int, pattern string) []string
}