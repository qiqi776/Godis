package command

import (
	"sort"
	"strconv"
	"strings"

	"godis/internal/resp"
)

func (e *Executor) execType(session Session, args [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	return resp.SimpleString(db.Type(string(args[0])))
}

func (e *Executor) execDBSize(session Session, _ [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	return resp.Integer(db.DBSize())
}

func (e *Executor) execInfo(session Session, _ [][]byte) []byte {
	db := e.engine.DB(session.GetDBIndex())
	if db == nil {
		return resp.Error("ERR DB index is out of range")
	}

	return resp.BulkString(buildInfo(session.GetDBIndex(), db.DBSize(), e.engine.DBCount()))
}

func (e *Executor) execCommand(_ Session, _ [][]byte) []byte {
	return resp.ArrayBulkStrings(e.commandNames())
}

func (e *Executor) commandNames() [][]byte {
	seen := make(map[string]struct{}, len(e.commands)+3)

	for name := range e.commands {
		seen[strings.ToLower(name)] = struct{}{}
	}

	// Pub/Sub 在 server 层处理，但项目实际支持它们
	seen["subscribe"] = struct{}{}
	seen["unsubscribe"] = struct{}{}
	seen["publish"] = struct{}{}

	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([][]byte, 0, len(names))
	for _, name := range names {
		out = append(out, []byte(name))
	}
	return out
}

func buildInfo(dbIndex int, dbSize int64, dbCount int) []byte {
	var builder strings.Builder

	builder.WriteString("# Server\r\n")
	builder.WriteString("godis_version:0.1\r\n")
	builder.WriteString("mode:standalone\r\n")

	builder.WriteString("# Keyspace\r\n")
	builder.WriteString("databases:")
	builder.WriteString(strconv.Itoa(dbCount))
	builder.WriteString("\r\n")

	builder.WriteString("db")
	builder.WriteString(strconv.Itoa(dbIndex))
	builder.WriteString(":keys=")
	builder.WriteString(strconv.FormatInt(dbSize, 10))
	builder.WriteString("\r\n")

	return []byte(builder.String())
}
