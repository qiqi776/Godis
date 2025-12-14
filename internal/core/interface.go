package core

import (
	"godis/internal/aof"
	"godis/internal/database"
	"godis/pkg/protocol"
	"time"
)

type Context struct {
	Args []protocol.Value
	DB   KVStorage
}

type CommandFunc func(ctx *Context) protocol.Value

type KVStorage interface {
	Get(key string) ([]byte, bool)
	Set(key string, value []byte)

	SetExpiration(key string, ddl time.Time) (bool, error)
	RmExpiration(key string) (bool, error)
	GetTTL(key string) (time.Duration, bool, error)

	GetStats() *database.Stats
	KeyCount() int

	GetAof() *aof.Aof
	SetAof(aof *aof.Aof)
}