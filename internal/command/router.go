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
	InMulti() bool
	StartMulti() bool
	Queue([][]byte)
	Queued() [][][]byte
	ClearMulti()
    Watch(int, string, uint64)
    Watched() map[int]map[string]uint64
    ClearWatch()
}

type Executor struct {
	engine   *engine.Engine
	commands map[string]Meta
}

func NewExecutor(eng *engine.Engine) *Executor {
	e := &Executor{
		engine:   eng,
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
	e.register("HSET", 3, 3, e.execHSet)
	e.register("HGET", 2, 2, e.execHGet)
	e.register("HDEL", 2, -1, e.execHDel)
	e.register("HGETALL", 1, 1, e.execHGetAll)
	e.register("SADD", 2, -1, e.execSAdd)
	e.register("SREM", 2, -1, e.execSRem)
	e.register("SMEMBERS", 1, 1, e.execSMembers)
	e.register("SISMEMBER", 2, 2, e.execSIsMember)
	e.register("ZADD", 3, 3, e.execZAdd)
	e.register("ZREM", 2, -1, e.execZRem)
	e.register("ZRANGE", 3, 3, e.execZRange)
	e.register("ZSCORE", 2, 2, e.execZScore)
	e.register("SETBIT", 3, 3, e.execSetBit)
	e.register("GETBIT", 2, 2, e.execGetBit)
	e.register("BITCOUNT", 1, 1, e.execBitCount)
	e.register("MULTI", 0, 0, e.execMulti)
	e.register("EXEC", 0, 0, e.execExec)
	e.register("DISCARD", 0, 0, e.execDiscard)
	e.register("WATCH", 1, -1, e.execWatch)
	e.register("UNWATCH", 0, 0, e.execUnwatch)
	e.register("TYPE", 1, 1, e.execType)
	e.register("DBSIZE", 0, 0, e.execDBSize)
	e.register("INFO", 0, 0, e.execInfo)
	e.register("COMMAND", 0, 0, e.execCommand)

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

	switch name {
	case "MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH":
		return meta.Exec(session, args)
	}
	if session.InMulti() {
		session.Queue(tokens)
		return resp.SimpleString("QUEUED")
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

func (e *Executor) execHSet(session Session, args [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	n, err := db.HSet(string(args[0]), string(args[1]), args[2])
	if err != nil {
		return resp.Error(err.Error())
	}
	return resp.Integer(n)
}

func (e *Executor) execHGet(session Session, args [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	value, ok, err := db.HGet(string(args[0]), string(args[1]))
	if err != nil {
		return resp.Error(err.Error())
	}
	if !ok {
		return resp.NullBulkString()
	}
	return resp.BulkString(value)
}

func (e *Executor) execHDel(session Session, args [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	fields := make([]string, 0, len(args)-1)
	for _, arg := range args[1:] {
		fields = append(fields, string(arg))
	}

	n, err := db.HDel(string(args[0]), fields...)
	if err != nil {
		return resp.Error(err.Error())
	}
	return resp.Integer(n)
}

func (e *Executor) execHGetAll(session Session, args [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	values, err := db.HGetAll(string(args[0]))
	if err != nil {
		return resp.Error(err.Error())
	}
	return resp.ArrayBulkStrings(values)
}

func (e *Executor) execSAdd(session Session, args [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	members := make([]string, 0, len(args)-1)
	for _, arg := range args[1:] {
		members = append(members, string(arg))
	}

	n, err := db.SAdd(string(args[0]), members...)
	if err != nil {
		return resp.Error(err.Error())
	}
	return resp.Integer(n)
}

func (e *Executor) execSRem(session Session, args [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	members := make([]string, 0, len(args)-1)
	for _, arg := range args[1:] {
		members = append(members, string(arg))
	}

	n, err := db.SRem(string(args[0]), members...)
	if err != nil {
		return resp.Error(err.Error())
	}
	return resp.Integer(n)
}

func (e *Executor) execSMembers(session Session, args [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	values, err := db.SMembers(string(args[0]))
	if err != nil {
		return resp.Error(err.Error())
	}
	return resp.ArrayBulkStrings(values)
}

func (e *Executor) execSIsMember(session Session, args [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	ok, err := db.SIsMember(string(args[0]), string(args[1]))
	if err != nil {
		return resp.Error(err.Error())
	}
	if ok {
		return resp.Integer(1)
	}
	return resp.Integer(0)
}

func (e *Executor) execZAdd(session Session, args [][]byte) []byte {
	score, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return resp.Error("ERR value is not a valid float")
	}

	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	n, err := db.ZAdd(string(args[0]), score, string(args[2]))
	if err != nil {
		return resp.Error(err.Error())
	}
	return resp.Integer(n)
}

func (e *Executor) execZRem(session Session, args [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	members := make([]string, 0, len(args)-1)
	for _, arg := range args[1:] {
		members = append(members, string(arg))
	}

	n, err := db.ZRem(string(args[0]), members...)
	if err != nil {
		return resp.Error(err.Error())
	}
	return resp.Integer(n)
}

func (e *Executor) execZRange(session Session, args [][]byte) []byte {
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

	values, err := db.ZRange(string(args[0]), start, stop)
	if err != nil {
		return resp.Error(err.Error())
	}
	return resp.ArrayBulkStrings(values)
}

func (e *Executor) execZScore(session Session, args [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	score, ok, err := db.ZScore(string(args[0]), string(args[1]))
	if err != nil {
		return resp.Error(err.Error())
	}
	if !ok {
		return resp.NullBulkString()
	}
	return resp.BulkString([]byte(strconv.FormatFloat(score, 'f', -1, 64)))
}

func (e *Executor) execSetBit(session Session, args [][]byte) []byte {
	offset, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return resp.Error("ERR bit offset is not an integer or out of range")
	}

	bit, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return resp.Error("ERR bit is not an integer or out of range")
	}
	if bit != 0 && bit != 1 {
		return resp.Error("ERR bit is not an integer or out of range")
	}

	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	old, err := db.SetBit(string(args[0]), offset, bit)
	if err != nil {
		return resp.Error(err.Error())
	}
	return resp.Integer(old)
}

func (e *Executor) execGetBit(session Session, args [][]byte) []byte {
	offset, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return resp.Error("ERR bit offset is not an integer or out of range")
	}

	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	bit, err := db.GetBit(string(args[0]), offset)
	if err != nil {
		return resp.Error(err.Error())
	}
	return resp.Integer(bit)
}

func (e *Executor) execBitCount(session Session, args [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	count, err := db.BitCount(string(args[0]))
	if err != nil {
		return resp.Error(err.Error())
	}
	return resp.Integer(count)
}

