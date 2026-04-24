package command

type Appender interface {
	Append(dbIndex int, tokens [][]byte) error
}

type Rewriter interface {
	Rewrite(snapshot func() [][][]byte) error
}

type Dumper interface {
	Dump(snapshot func() [][][]byte) error
}

func (e *Executor) SetAppender(appender Appender) {
	e.appender = appender
}

func (e *Executor) SetRewriter(rewriter Rewriter) {
	e.rewriter = rewriter
}

func (e *Executor) SetDumper(dumper Dumper) {
	e.dumper = dumper
}

func isWriteCommand(name string) bool {
	switch name {
	case "SET", "DEL", "EXPIRE", "PERSIST",
		"LPUSH", "RPUSH", "LPOP", "RPOP",
		"HSET", "HDEL",
		"SADD", "SREM",
		"ZADD", "ZREM",
		"SETBIT":
		return true
	default:
		return false
	}
}
