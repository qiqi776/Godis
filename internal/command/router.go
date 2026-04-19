package command

import (
	"bytes"
	"strconv"
	"strings"
	"time"

	"godis/internal/engine"
	"godis/internal/resp"
)

type Session interface {
	GetDBIndex() int
	SetDBIndex(int)
}

type Executor struct {
	engine   *engine.Engine
	commands map[string]Meta
}

func NewExecutor(eng *engine.Engine) *Executor {
	e := &Executor{
		engine: eng,
		commands: make(map[string]Meta),
	}
	e.registerBase()
	return e
}

func (e *Executor) register(name string, minArgs int, maxArgs int, exec Handler) {
	e.commands[name] = Meta{
		MinArgs: minArgs,
		MaxArgs: maxArgs,
		Exec:    exec,
	}
}

func (e *Executor) registerBase() {
	e.register("PING", 0, 1, e.execPing)
	e.register("GET", 1, 1, e.execGet)
	e.register("SET", 2, 2, e.execSet)
	e.register("DEL", 1, -1, e.execDel)
	e.register("EXISTS", 1, -1, e.execExists)
	e.register("EXPIRE", 2, 2, e.execExpire)
	e.register("TTL", 1, 1, e.execTTL)
	e.register("PERSIST", 1, 1, e.execPersist)
	e.register("SELECT", 1, 1, e.execSelect)
	e.register("LPUSH", 2, -1, e.execLPush)
	e.register("RPUSH", 2, -1, e.execRPush)
	e.register("LPOP", 1, 1, e.execLPop)
	e.register("RPOP", 1, 1, e.execRPop)
	e.register("LRANGE", 3, 3, e.execLRange)
}

func (e *Executor) Execute(session Session, tokens [][]byte) []byte {
	if len(tokens) == 0 {
		return resp.Error("ERR empty command")
	}

	name := strings.ToUpper(string(tokens[0]))
	meta, ok := e.commands[name]
	if !ok {
		return resp.Error("ERR unknown command '" + strings.ToLower(name) + "'")
	}

	args := tokens[1:]
	if !meta.Match(len(args)) {
		return wrongArity(strings.ToLower(name))
	}
	return meta.Exec(session, args)
}

func (e *Executor) execPing(_ Session, args [][]byte) []byte {
	if len(args) == 1 {
		return resp.BulkString(args[0])
	}
	return resp.SimpleString("PONG")
}

func (e *Executor) execGet(session Session, args [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	value, ok, err := db.Get(string(args[0]))
	if err != nil {
		return resp.Error(err.Error())
	}
	if !ok {
		return resp.NullBulkString()
	}
	return resp.BulkString(value)
}

func (e *Executor) execSet(session Session, args [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	db.Set(string(args[0]), args[1])
	return resp.SimpleString("OK")
}

func (e *Executor) execDel(session Session, args [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
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
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	keys := make([]string, 0, len(args))
	for _, arg := range args {
		keys = append(keys, string(arg))
	}

	return resp.Integer(db.Exists(keys...))
}

func (e *Executor) execExpire(session Session, args [][]byte) []byte {
	seconds, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return resp.Error("ERR value is not an integer or out of range")
	}

	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	if db.Expire(string(args[0]), time.Duration(seconds)*time.Second) {
		return resp.Integer(1)
	}
	return resp.Integer(0)
}

func (e *Executor) execTTL(session Session, args [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	return resp.Integer(db.TTL(string(args[0])))
}

func (e *Executor) execPersist(session Session, args [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	if db.Persist(string(args[0])) {
		return resp.Integer(1)
	}
	return resp.Integer(0)
}

func (e *Executor) execSelect(session Session, args [][]byte) []byte {
	index, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return resp.Error("ERR value is not an integer or out of range")
	}
	if e.engine.DB(index) == nil {
		return resp.Error("ERR DB index is out of range")
	}

	session.SetDBIndex(index)
	return resp.SimpleString("OK")
}

func (e *Executor) execLPush(session Session, args [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	n, err := db.LPush(string(args[0]), args[1:]...)
	if err != nil {
		return resp.Error(err.Error())
	}
	return resp.Integer(n)
}

func (e *Executor) execRPush(session Session, args [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	n, err := db.RPush(string(args[0]), args[1:]...)
	if err != nil {
		return resp.Error(err.Error())
	}
	return resp.Integer(n)
}

func (e *Executor) execLPop(session Session, args [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	value, ok, err := db.LPop(string(args[0]))
	if err != nil {
		return resp.Error(err.Error())
	}
	if !ok {
		return resp.NullBulkString()
	}
	return resp.BulkString(value)
}

func (e *Executor) execRPop(session Session, args [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	value, ok, err := db.RPop(string(args[0]))
	if err != nil {
		return resp.Error(err.Error())
	}
	if !ok {
		return resp.NullBulkString()
	}
	return resp.BulkString(value)
}

func (e *Executor) execLRange(session Session, args [][]byte) []byte {
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return resp.Error("ERR value is not an integer or out of range")
	}

	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return resp.Error("ERR value is not an integer or out of range")
	}

	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	values, err := db.LRange(string(args[0]), start, stop)
	if err != nil {
		return resp.Error(err.Error())
	}
	return resp.ArrayBulkStrings(values)
}

func wrongArity(command string) []byte {
	var buf bytes.Buffer
	buf.WriteString("ERR wrong number of arguments for '")
	buf.WriteString(command)
	buf.WriteString("' command")
	return resp.Error(buf.String())
}
