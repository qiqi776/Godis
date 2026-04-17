package command

import (
	"bytes"
	"godis/internal/resp"
	"strings"
)

type Session interface {
	GetIndex() int
	SetIndex(int)
}

type Executor struct {}

func NewExecutor() *Executor {
	return &Executor{}
}

func (e *Executor) Execute(session Session, tokens [][]byte) []byte {
	_ = session
	if len(tokens) == 0 {
		return resp.Error("ERR empty command")
	}

	name := strings.ToUpper(string(tokens[0]))
	args := tokens[1:]

	switch name {
	case "PING":
		return execPing(args)
	default:
		return resp.Error("ERR unknown command '" + strings.ToLower(name) + "'")
	}
}

func execPing(args [][]byte) []byte {
    if len(args) > 1 {
        return wrongArity("ping")
    }
    if len(args) == 1 {
        return resp.BulkString(args[0])
    }
    return resp.SimpleString("PONG")
}

func wrongArity(command string) []byte {
    var buf bytes.Buffer
    buf.WriteString("ERR wrong number of arguments for '")
    buf.WriteString(command)
    buf.WriteString("' command")
    return resp.Error(buf.String())
}