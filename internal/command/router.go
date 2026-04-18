package command

import (
	"bytes"
	"godis/internal/engine"
	"godis/internal/resp"
	"strings"
)

type Session interface {
	GetIndex() int
	SetIndex(int)
}

type Executor struct {
	engine *engine.Engine
}

func NewExecutor(eng *engine.Engine) *Executor {
	return &Executor{
		engine: eng,
	}
}

func (e *Executor) Execute(session Session, tokens [][]byte) []byte {
	if len(tokens) == 0 {
		return resp.Error("ERR empty command")
	}

	name := strings.ToUpper(string(tokens[0]))
	args := tokens[1:]

	switch name {
    case "PING":
        return e.execPing(args)
    case "GET":
        return e.execGet(session, args)
    case "SET":
        return e.execSet(session, args)
    case "DEL":
        return e.execDel(session, args)
    case "EXISTS":
        return e.execExists(session, args)
    default:
        return resp.Error("ERR unknown command '" + strings.ToLower(name) + "'")
    }
}

func (e *Executor) execPing(args [][]byte) []byte {
    if len(args) > 1 {
        return wrongArity("ping")
    }
    if len(args) == 1 {
        return resp.BulkString(args[0])
    }
    return resp.SimpleString("PONG")
}

func (e *Executor) execGet(session Session, args [][]byte) []byte {
    if len(args) != 1 {
        return wrongArity("get")
    }

    db := e.engine.DB(session.GetIndex())
    if db == nil {
        return resp.Error("ERR DB index is out of range")
    }

    value, ok := db.Get(string(args[0]))
    if !ok {
        return resp.NullBulkString()
    }
    return resp.BulkString(value)
}

func (e *Executor) execSet(session Session, args [][]byte) []byte {
    if len(args) != 2 {
        return wrongArity("set")
    }

    db := e.engine.DB(session.GetIndex())
    if db == nil {
        return resp.Error("ERR DB index is out of range")
    }

    db.Set(string(args[0]), args[1])
    return resp.SimpleString("OK")
}

func (e *Executor) execDel(session Session, args [][]byte) []byte {
    if len(args) < 1 {
        return wrongArity("del")
    }

    db := e.engine.DB(session.GetIndex())
    if db == nil {
        return resp.Error("ERR DB index is out of range")
    }

    keys := make([]string, 0, len(args))
    for _, arg := range args {
        keys = append(keys, string(arg))
    }

    return resp.Integer(db.Del(keys...))
}

func (e *Executor) execExists(session Session, args [][]byte) []byte {
    if len(args) < 1 {
        return wrongArity("exists")
    }

    db := e.engine.DB(session.GetIndex())
    if db == nil {
        return resp.Error("ERR DB index is out of range")
    }

    keys := make([]string, 0, len(args))
    for _, arg := range args {
        keys = append(keys, string(arg))
    }

    return resp.Integer(db.Exists(keys...))
}


func wrongArity(command string) []byte {
    var buf bytes.Buffer
    buf.WriteString("ERR wrong number of arguments for '")
    buf.WriteString(command)
    buf.WriteString("' command")
    return resp.Error(buf.String())
}