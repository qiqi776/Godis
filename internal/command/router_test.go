package command

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"godis/internal/engine"
)

type testSession struct {
    dbIndex int
    inMulti bool
    queued  [][][]byte
    watched map[int]map[string]uint64
}

func (s *testSession) GetDBIndex() int {
	return s.dbIndex
}

func (s *testSession) SetDBIndex(index int) {
	s.dbIndex = index
}

func (s *testSession) InMulti() bool {
	return s.inMulti
}

func (s *testSession) StartMulti() bool {
	if s.inMulti {
		return false
	}
	s.inMulti = true
	s.queued = nil
	return true
}

func (s *testSession) Queue(tokens [][]byte) {
	out := make([][]byte, 0, len(tokens))
	for _, token := range tokens {
		out = append(out, append([]byte(nil), token...))
	}
	s.queued = append(s.queued, out)
}

func (s *testSession) Queued() [][][]byte {
	return s.queued
}

func (s *testSession) ClearMulti() {
	s.inMulti = false
	s.queued = nil
}

func (s *testSession) Watch(dbIndex int, key string, rev uint64) {
    if s.watched == nil {
        s.watched = make(map[int]map[string]uint64)
    }
    if s.watched[dbIndex] == nil {
        s.watched[dbIndex] = make(map[string]uint64)
    }
    s.watched[dbIndex][key] = rev
}

func (s *testSession) Watched() map[int]map[string]uint64 {
    return s.watched
}

func (s *testSession) ClearWatch() {
    s.watched = nil
}

func TestPing(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	got := string(exec.Execute(sess, cmd("PING")))
	want := "+PONG\r\n"
	if got != want {
		t.Fatalf("want %q, got %q", want, got)
	}
}

func TestPingEcho(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	got := string(exec.Execute(sess, cmd("PING", "hello")))
	want := "$5\r\nhello\r\n"
	if got != want {
		t.Fatalf("want %q, got %q", want, got)
	}
}

func TestSetGet(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	if got := string(exec.Execute(sess, cmd("SET", "a", "1"))); got != "+OK\r\n" {
		t.Fatalf("unexpected SET reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("GET", "a"))); got != "$1\r\n1\r\n" {
		t.Fatalf("unexpected GET reply: %q", got)
	}
}

func TestDelExists(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	exec.Execute(sess, cmd("SET", "a", "1"))
	exec.Execute(sess, cmd("SET", "b", "2"))

	if got := string(exec.Execute(sess, cmd("EXISTS", "a", "b", "c"))); got != ":2\r\n" {
		t.Fatalf("unexpected EXISTS reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("DEL", "a", "b"))); got != ":2\r\n" {
		t.Fatalf("unexpected DEL reply: %q", got)
	}
}

func TestTTL(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	exec.Execute(sess, cmd("SET", "k", "v"))

	if got := string(exec.Execute(sess, cmd("TTL", "k"))); got != ":-1\r\n" {
		t.Fatalf("unexpected TTL reply before expire: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("EXPIRE", "k", "1"))); got != ":1\r\n" {
		t.Fatalf("unexpected EXPIRE reply: %q", got)
	}

	raw := strings.TrimSuffix(strings.TrimPrefix(string(exec.Execute(sess, cmd("TTL", "k"))), ":"), "\r\n")
	ttl, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse TTL reply: %v", err)
	}
	if ttl < 1 {
		t.Fatalf("expected positive TTL, got %d", ttl)
	}

	time.Sleep(1100 * time.Millisecond)

	if got := string(exec.Execute(sess, cmd("TTL", "k"))); got != ":-2\r\n" {
		t.Fatalf("unexpected TTL reply after expire: %q", got)
	}
}

func TestSelect(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	exec.Execute(sess, cmd("SET", "a", "1"))

	if got := string(exec.Execute(sess, cmd("SELECT", "1"))); got != "+OK\r\n" {
		t.Fatalf("unexpected SELECT reply: %q", got)
	}
	if sess.dbIndex != 1 {
		t.Fatalf("expected db index 1, got %d", sess.dbIndex)
	}

	if got := string(exec.Execute(sess, cmd("GET", "a"))); got != "$-1\r\n" {
		t.Fatalf("unexpected GET reply in db1: %q", got)
	}

	exec.Execute(sess, cmd("SET", "a", "2"))
	exec.Execute(sess, cmd("SELECT", "0"))

	if got := string(exec.Execute(sess, cmd("GET", "a"))); got != "$1\r\n1\r\n" {
		t.Fatalf("unexpected GET reply in db0: %q", got)
	}
}

func TestSelectErr(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	if got := string(exec.Execute(sess, cmd("SELECT", "x"))); got != "-ERR value is not an integer or out of range\r\n" {
		t.Fatalf("unexpected SELECT type error: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("SELECT", "9"))); got != "-ERR DB index is out of range\r\n" {
		t.Fatalf("unexpected SELECT range error: %q", got)
	}
}

func TestArity(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	if got := string(exec.Execute(sess, cmd("GET"))); got != "-ERR wrong number of arguments for 'get' command\r\n" {
		t.Fatalf("unexpected GET arity error: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("PING", "a", "b"))); got != "-ERR wrong number of arguments for 'ping' command\r\n" {
		t.Fatalf("unexpected PING arity error: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("DEL"))); got != "-ERR wrong number of arguments for 'del' command\r\n" {
		t.Fatalf("unexpected DEL arity error: %q", got)
	}
}

func TestUnknown(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	got := string(exec.Execute(sess, cmd("NOPE")))
	want := "-ERR unknown command 'nope'\r\n"
	if got != want {
		t.Fatalf("want %q, got %q", want, got)
	}
}

func cmd(parts ...string) [][]byte {
	out := make([][]byte, 0, len(parts))
	for _, part := range parts {
		out = append(out, []byte(part))
	}
	return out
}

func TestList(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	if got := string(exec.Execute(sess, cmd("LPUSH", "list", "b", "a"))); got != ":2\r\n" {
		t.Fatalf("unexpected LPUSH reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("RPUSH", "list", "c"))); got != ":3\r\n" {
		t.Fatalf("unexpected RPUSH reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("LRANGE", "list", "0", "-1"))); got != "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n" {
		t.Fatalf("unexpected LRANGE reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("LPOP", "list"))); got != "$1\r\na\r\n" {
		t.Fatalf("unexpected LPOP reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("RPOP", "list"))); got != "$1\r\nc\r\n" {
		t.Fatalf("unexpected RPOP reply: %q", got)
	}
}

func TestType(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	exec.Execute(sess, cmd("SET", "key", "value"))

	if got := string(exec.Execute(sess, cmd("LPUSH", "key", "a"))); got != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
		t.Fatalf("unexpected LPUSH wrongtype reply: %q", got)
	}
}

func TestHash(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	if got := string(exec.Execute(sess, cmd("HSET", "user", "name", "godis"))); got != ":1\r\n" {
		t.Fatalf("unexpected HSET reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("HGET", "user", "name"))); got != "$5\r\ngodis\r\n" {
		t.Fatalf("unexpected HGET reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("HSET", "user", "name", "redis"))); got != ":0\r\n" {
		t.Fatalf("unexpected HSET update reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("HGETALL", "user"))); got != "*2\r\n$4\r\nname\r\n$5\r\nredis\r\n" {
		t.Fatalf("unexpected HGETALL reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("HDEL", "user", "name"))); got != ":1\r\n" {
		t.Fatalf("unexpected HDEL reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("HGET", "user", "name"))); got != "$-1\r\n" {
		t.Fatalf("unexpected HGET missing reply: %q", got)
	}
}

func TestHashType(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	exec.Execute(sess, cmd("SET", "key", "value"))

	if got := string(exec.Execute(sess, cmd("HSET", "key", "field", "x"))); got != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
		t.Fatalf("unexpected HSET wrongtype reply: %q", got)
	}
}

func TestSetData(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	if got := string(exec.Execute(sess, cmd("SADD", "tags", "go", "redis", "go"))); got != ":2\r\n" {
		t.Fatalf("unexpected SADD reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("SISMEMBER", "tags", "go"))); got != ":1\r\n" {
		t.Fatalf("unexpected SISMEMBER reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("SMEMBERS", "tags"))); got != "*2\r\n$2\r\ngo\r\n$5\r\nredis\r\n" {
		t.Fatalf("unexpected SMEMBERS reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("SREM", "tags", "go"))); got != ":1\r\n" {
		t.Fatalf("unexpected SREM reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("SISMEMBER", "tags", "go"))); got != ":0\r\n" {
		t.Fatalf("unexpected SISMEMBER reply after remove: %q", got)
	}
}

func TestSetType(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	exec.Execute(sess, cmd("SET", "key", "value"))

	if got := string(exec.Execute(sess, cmd("SADD", "key", "a"))); got != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
		t.Fatalf("unexpected SADD wrongtype reply: %q", got)
	}
}

func TestZSet(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	if got := string(exec.Execute(sess, cmd("ZADD", "rank", "1", "a"))); got != ":1\r\n" {
		t.Fatalf("unexpected ZADD reply: %q", got)
	}
	if got := string(exec.Execute(sess, cmd("ZADD", "rank", "2", "b"))); got != ":1\r\n" {
		t.Fatalf("unexpected ZADD reply: %q", got)
	}
	if got := string(exec.Execute(sess, cmd("ZADD", "rank", "3", "a"))); got != ":0\r\n" {
		t.Fatalf("unexpected ZADD update reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("ZSCORE", "rank", "a"))); got != "$1\r\n3\r\n" {
		t.Fatalf("unexpected ZSCORE reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("ZRANGE", "rank", "0", "-1"))); got != "*2\r\n$1\r\nb\r\n$1\r\na\r\n" {
		t.Fatalf("unexpected ZRANGE reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("ZREM", "rank", "b"))); got != ":1\r\n" {
		t.Fatalf("unexpected ZREM reply: %q", got)
	}
}

func TestZSetType(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	exec.Execute(sess, cmd("SET", "key", "value"))

	if got := string(exec.Execute(sess, cmd("ZADD", "key", "1", "a"))); got != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
		t.Fatalf("unexpected ZADD wrongtype reply: %q", got)
	}
}

func TestBitmap(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	if got := string(exec.Execute(sess, cmd("SETBIT", "bits", "7", "1"))); got != ":0\r\n" {
		t.Fatalf("unexpected SETBIT reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("GETBIT", "bits", "7"))); got != ":1\r\n" {
		t.Fatalf("unexpected GETBIT reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("BITCOUNT", "bits"))); got != ":1\r\n" {
		t.Fatalf("unexpected BITCOUNT reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("SETBIT", "bits", "7", "0"))); got != ":1\r\n" {
		t.Fatalf("unexpected SETBIT reset reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("BITCOUNT", "bits"))); got != ":0\r\n" {
		t.Fatalf("unexpected BITCOUNT after reset: %q", got)
	}
}

func TestBitmapType(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	exec.Execute(sess, cmd("SET", "key", "value"))

	if got := string(exec.Execute(sess, cmd("SETBIT", "key", "1", "1"))); got != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
		t.Fatalf("unexpected SETBIT wrongtype reply: %q", got)
	}
}

func TestTx(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	if got := string(exec.Execute(sess, cmd("MULTI"))); got != "+OK\r\n" {
		t.Fatalf("unexpected MULTI reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("SET", "a", "1"))); got != "+QUEUED\r\n" {
		t.Fatalf("unexpected queued SET reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("GET", "a"))); got != "+QUEUED\r\n" {
		t.Fatalf("unexpected queued GET reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("EXEC"))); got != "*2\r\n+OK\r\n$1\r\n1\r\n" {
		t.Fatalf("unexpected EXEC reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("GET", "a"))); got != "$1\r\n1\r\n" {
		t.Fatalf("unexpected GET after EXEC reply: %q", got)
	}
}

func TestDiscard(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	if got := string(exec.Execute(sess, cmd("MULTI"))); got != "+OK\r\n" {
		t.Fatalf("unexpected MULTI reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("SET", "a", "1"))); got != "+QUEUED\r\n" {
		t.Fatalf("unexpected queued SET reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("DISCARD"))); got != "+OK\r\n" {
		t.Fatalf("unexpected DISCARD reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("GET", "a"))); got != "$-1\r\n" {
		t.Fatalf("unexpected GET after DISCARD reply: %q", got)
	}
}

func TestTxErr(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	if got := string(exec.Execute(sess, cmd("EXEC"))); got != "-ERR EXEC without MULTI\r\n" {
		t.Fatalf("unexpected EXEC error reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("DISCARD"))); got != "-ERR DISCARD without MULTI\r\n" {
		t.Fatalf("unexpected DISCARD error reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("MULTI"))); got != "+OK\r\n" {
		t.Fatalf("unexpected first MULTI reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("MULTI"))); got != "-ERR MULTI calls can not be nested\r\n" {
		t.Fatalf("unexpected nested MULTI reply: %q", got)
	}
}

func TestWatch(t *testing.T) {
    t.Parallel()

    exec := NewExecutor(engine.New(2))
    s1 := &testSession{}
    s2 := &testSession{}

    if got := string(exec.Execute(s1, cmd("WATCH", "a"))); got != "+OK\r\n" {
        t.Fatalf("unexpected WATCH reply: %q", got)
    }

    if got := string(exec.Execute(s1, cmd("MULTI"))); got != "+OK\r\n" {
        t.Fatalf("unexpected MULTI reply: %q", got)
    }

    if got := string(exec.Execute(s1, cmd("GET", "a"))); got != "+QUEUED\r\n" {
        t.Fatalf("unexpected queued GET reply: %q", got)
    }

    if got := string(exec.Execute(s2, cmd("SET", "a", "1"))); got != "+OK\r\n" {
        t.Fatalf("unexpected SET reply from second session: %q", got)
    }

    if got := string(exec.Execute(s1, cmd("EXEC"))); got != "*-1\r\n" {
        t.Fatalf("unexpected EXEC abort reply: %q", got)
    }
}

func TestWatchErr(t *testing.T) {
    t.Parallel()

    exec := NewExecutor(engine.New(2))
    sess := &testSession{}

    if got := string(exec.Execute(sess, cmd("MULTI"))); got != "+OK\r\n" {
        t.Fatalf("unexpected MULTI reply: %q", got)
    }

    if got := string(exec.Execute(sess, cmd("WATCH", "a"))); got != "-ERR WATCH inside MULTI is not allowed\r\n" {
        t.Fatalf("unexpected WATCH error reply: %q", got)
    }
}

func TestUnwatch(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	s1 := &testSession{}
	s2 := &testSession{}

	if got := string(exec.Execute(s1, cmd("WATCH", "a"))); got != "+OK\r\n" {
		t.Fatalf("unexpected WATCH reply: %q", got)
	}

	if got := string(exec.Execute(s1, cmd("UNWATCH"))); got != "+OK\r\n" {
		t.Fatalf("unexpected UNWATCH reply: %q", got)
	}

	if got := string(exec.Execute(s1, cmd("MULTI"))); got != "+OK\r\n" {
		t.Fatalf("unexpected MULTI reply: %q", got)
	}

	if got := string(exec.Execute(s1, cmd("GET", "a"))); got != "+QUEUED\r\n" {
		t.Fatalf("unexpected queued GET reply: %q", got)
	}

	if got := string(exec.Execute(s2, cmd("SET", "a", "1"))); got != "+OK\r\n" {
		t.Fatalf("unexpected SET reply from second session: %q", got)
	}

	if got := string(exec.Execute(s1, cmd("EXEC"))); got != "*1\r\n$1\r\n1\r\n" {
		t.Fatalf("unexpected EXEC reply after UNWATCH: %q", got)
	}
}

func TestUnwatchErr(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	if got := string(exec.Execute(sess, cmd("UNWATCH"))); got != "+OK\r\n" {
		t.Fatalf("unexpected UNWATCH reply without watch: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("MULTI"))); got != "+OK\r\n" {
		t.Fatalf("unexpected MULTI reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("UNWATCH"))); got != "-ERR UNWATCH inside MULTI is not allowed\r\n" {
		t.Fatalf("unexpected UNWATCH error reply: %q", got)
	}
}
