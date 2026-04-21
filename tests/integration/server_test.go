package integration

import (
	"bufio"
	"context"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"godis/internal/command"
	"godis/internal/common/logger"
	"godis/internal/config"
	"godis/internal/engine"
	"godis/internal/server"
)

func newTestClient(t *testing.T) (net.Conn, *bufio.Reader) {
	t.Helper()

	cfg := config.Config{
		Host:      "127.0.0.1",
		Port:      0,
		LogLevel:  "error",
		Databases: 4,
	}

	eng := engine.New(cfg.Databases)
	t.Cleanup(eng.Close)

	exec := command.NewExecutor(eng)
	srv := server.New(cfg, logger.NewDiscard(), exec)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Run(ctx)
	}()

	addr := waitAddr(t, srv)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		cancel()
		t.Fatalf("dial server: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		cancel()

		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("server shutdown: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("server did not stop")
		}
	})

	return conn, bufio.NewReader(conn)
}

func TestKV(t *testing.T) {
	t.Parallel()

	conn, reader := newTestClient(t)

	writeCmd(t, conn, "PING")
	wantReply(t, reader, "+PONG\r\n")

	writeCmd(t, conn, "SET", "demo", "42")
	wantReply(t, reader, "+OK\r\n")

	writeCmd(t, conn, "GET", "demo")
	wantReply(t, reader, "$2\r\n42\r\n")

	writeCmd(t, conn, "EXISTS", "demo")
	wantReply(t, reader, ":1\r\n")

	writeCmd(t, conn, "DEL", "demo")
	wantReply(t, reader, ":1\r\n")

	writeCmd(t, conn, "GET", "demo")
	wantReply(t, reader, "$-1\r\n")

	writeCmd(t, conn, "NOPE")
	wantReply(t, reader, "-ERR unknown command 'nope'\r\n")

}

func TestTTL(t *testing.T) {
	t.Parallel()

	conn, reader := newTestClient(t)

	writeCmd(t, conn, "SET", "temp", "42")
	wantReply(t, reader, "+OK\r\n")

	writeCmd(t, conn, "TTL", "temp")
	wantReply(t, reader, ":-1\r\n")

	writeCmd(t, conn, "EXPIRE", "temp", "2")
	wantReply(t, reader, ":1\r\n")

	writeCmd(t, conn, "TTL", "temp")
	wantInt(t, reader, 1, 2)

	writeCmd(t, conn, "PERSIST", "temp")
	wantReply(t, reader, ":1\r\n")

	writeCmd(t, conn, "TTL", "temp")
	wantReply(t, reader, ":-1\r\n")

	writeCmd(t, conn, "EXPIRE", "temp", "1")
	wantReply(t, reader, ":1\r\n")

	time.Sleep(1100 * time.Millisecond)

	writeCmd(t, conn, "GET", "temp")
	wantReply(t, reader, "$-1\r\n")

	writeCmd(t, conn, "TTL", "temp")
	wantReply(t, reader, ":-2\r\n")

	writeCmd(t, conn, "PERSIST", "missing")
	wantReply(t, reader, ":0\r\n")

}

func TestSelect(t *testing.T) {
	t.Parallel()

	conn, reader := newTestClient(t)

	writeCmd(t, conn, "SET", "a", "1")
	wantReply(t, reader, "+OK\r\n")

	writeCmd(t, conn, "SELECT", "1")
	wantReply(t, reader, "+OK\r\n")

	writeCmd(t, conn, "GET", "a")
	wantReply(t, reader, "$-1\r\n")

	writeCmd(t, conn, "SET", "a", "2")
	wantReply(t, reader, "+OK\r\n")

	writeCmd(t, conn, "GET", "a")
	wantReply(t, reader, "$1\r\n2\r\n")

	writeCmd(t, conn, "SELECT", "0")
	wantReply(t, reader, "+OK\r\n")

	writeCmd(t, conn, "GET", "a")
	wantReply(t, reader, "$1\r\n1\r\n")

	writeCmd(t, conn, "SELECT", "9")
	wantReply(t, reader, "-ERR DB index is out of range\r\n")

	writeCmd(t, conn, "SELECT", "abc")
	wantReply(t, reader, "-ERR value is not an integer or out of range\r\n")

}

func waitAddr(t *testing.T, srv *server.Server) string {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if addr := srv.Addr(); addr != "" {
			return addr
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("server did not expose listen address in time")
	return ""
}

func writeCmd(t *testing.T, conn net.Conn, parts ...string) {
	t.Helper()

	var builder strings.Builder
	builder.WriteString("*")
	builder.WriteString(strconv.Itoa(len(parts)))
	builder.WriteString("\r\n")

	for _, part := range parts {
		builder.WriteString("$")
		builder.WriteString(strconv.Itoa(len(part)))
		builder.WriteString("\r\n")
		builder.WriteString(part)
		builder.WriteString("\r\n")
	}

	if _, err := conn.Write([]byte(builder.String())); err != nil {
		t.Fatalf("write command: %v", err)
	}
}

func wantReply(t *testing.T, reader *bufio.Reader, want string) {
	t.Helper()

	got := readResp(t, reader)
	if got != want {
		t.Fatalf("unexpected reply\nwant: %q\ngot:  %q", want, got)
	}
}

func wantInt(t *testing.T, reader *bufio.Reader, min, max int64) {
	t.Helper()

	got := readResp(t, reader)
	if len(got) < 3 || got[0] != ':' {
		t.Fatalf("expected integer reply, got %q", got)
	}

	value, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(got, ":"), "\r\n"), 10, 64)
	if err != nil {
		t.Fatalf("parse integer reply: %v", err)
	}

	if value < min || value > max {
		t.Fatalf("integer reply out of range, want [%d,%d], got %d", min, max, value)
	}
}

func readResp(t *testing.T, reader *bufio.Reader) string {
	t.Helper()

	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read reply line: %v", err)
	}

	if len(line) == 0 {
		t.Fatal("empty reply")
	}

	if line[0] == '*' {
		size, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(line, "*"), "\r\n"))
		if err != nil {
			t.Fatalf("parse array reply: %v", err)
		}

		var builder strings.Builder
		builder.WriteString(line)
		for i := 0; i < size; i++ {
			builder.WriteString(readResp(t, reader))
		}
		return builder.String()
	}

	if line[0] != '$' {
		return line
	}

	sizeLine := strings.TrimSuffix(line, "\r\n")
	if sizeLine == "$-1" {
		return line
	}

	body, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read bulk body: %v", err)
	}

	return line + body
}

func TestList(t *testing.T) {
	t.Parallel()

	conn, reader := newTestClient(t)

	writeCmd(t, conn, "LPUSH", "list", "b", "a")
	wantReply(t, reader, ":2\r\n")

	writeCmd(t, conn, "RPUSH", "list", "c")
	wantReply(t, reader, ":3\r\n")

	writeCmd(t, conn, "LRANGE", "list", "0", "-1")
	wantReply(t, reader, "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n")

	writeCmd(t, conn, "LPOP", "list")
	wantReply(t, reader, "$1\r\na\r\n")

	writeCmd(t, conn, "RPOP", "list")
	wantReply(t, reader, "$1\r\nc\r\n")

	writeCmd(t, conn, "SET", "str", "1")
	wantReply(t, reader, "+OK\r\n")

	writeCmd(t, conn, "LPUSH", "str", "x")
	wantReply(t, reader, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

	writeCmd(t, conn, "EXPIRE", "list", "1")
	wantReply(t, reader, ":1\r\n")

	time.Sleep(1100 * time.Millisecond)

	writeCmd(t, conn, "LRANGE", "list", "0", "-1")
	wantReply(t, reader, "*0\r\n")

}

func TestHash(t *testing.T) {
	t.Parallel()

	conn, reader := newTestClient(t)

	writeCmd(t, conn, "HSET", "user", "name", "godis")
	wantReply(t, reader, ":1\r\n")

	writeCmd(t, conn, "HGET", "user", "name")
	wantReply(t, reader, "$5\r\ngodis\r\n")

	writeCmd(t, conn, "HSET", "user", "name", "redis")
	wantReply(t, reader, ":0\r\n")

	writeCmd(t, conn, "HGETALL", "user")
	wantReply(t, reader, "*2\r\n$4\r\nname\r\n$5\r\nredis\r\n")

	writeCmd(t, conn, "HDEL", "user", "name")
	wantReply(t, reader, ":1\r\n")

	writeCmd(t, conn, "HGET", "user", "name")
	wantReply(t, reader, "$-1\r\n")

	writeCmd(t, conn, "SET", "str", "1")
	wantReply(t, reader, "+OK\r\n")

	writeCmd(t, conn, "HSET", "str", "field", "x")
	wantReply(t, reader, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

}

func TestSetData(t *testing.T) {
	t.Parallel()

	conn, reader := newTestClient(t)

	writeCmd(t, conn, "SADD", "tags", "go", "redis", "go")
	wantReply(t, reader, ":2\r\n")

	writeCmd(t, conn, "SISMEMBER", "tags", "go")
	wantReply(t, reader, ":1\r\n")

	writeCmd(t, conn, "SMEMBERS", "tags")
	wantReply(t, reader, "*2\r\n$2\r\ngo\r\n$5\r\nredis\r\n")

	writeCmd(t, conn, "SREM", "tags", "go")
	wantReply(t, reader, ":1\r\n")

	writeCmd(t, conn, "SISMEMBER", "tags", "go")
	wantReply(t, reader, ":0\r\n")

	writeCmd(t, conn, "SET", "str", "1")
	wantReply(t, reader, "+OK\r\n")

	writeCmd(t, conn, "SADD", "str", "x")
	wantReply(t, reader, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

}

func TestZSet(t *testing.T) {
	t.Parallel()

	conn, reader := newTestClient(t)

	writeCmd(t, conn, "ZADD", "rank", "1", "a")
	wantReply(t, reader, ":1\r\n")

	writeCmd(t, conn, "ZADD", "rank", "2", "b")
	wantReply(t, reader, ":1\r\n")

	writeCmd(t, conn, "ZADD", "rank", "3", "a")
	wantReply(t, reader, ":0\r\n")

	writeCmd(t, conn, "ZSCORE", "rank", "a")
	wantReply(t, reader, "$1\r\n3\r\n")

	writeCmd(t, conn, "ZRANGE", "rank", "0", "-1")
	wantReply(t, reader, "*2\r\n$1\r\nb\r\n$1\r\na\r\n")

	writeCmd(t, conn, "ZREM", "rank", "b")
	wantReply(t, reader, ":1\r\n")

	writeCmd(t, conn, "SET", "str", "1")
	wantReply(t, reader, "+OK\r\n")

	writeCmd(t, conn, "ZADD", "str", "1", "x")
	wantReply(t, reader, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

}

func TestBitmap(t *testing.T) {
	t.Parallel()

	conn, reader := newTestClient(t)

	writeCmd(t, conn, "SETBIT", "bits", "7", "1")
	wantReply(t, reader, ":0\r\n")

	writeCmd(t, conn, "GETBIT", "bits", "7")
	wantReply(t, reader, ":1\r\n")

	writeCmd(t, conn, "BITCOUNT", "bits")
	wantReply(t, reader, ":1\r\n")

	writeCmd(t, conn, "SETBIT", "bits", "7", "0")
	wantReply(t, reader, ":1\r\n")

	writeCmd(t, conn, "BITCOUNT", "bits")
	wantReply(t, reader, ":0\r\n")

	writeCmd(t, conn, "SET", "str", "1")
	wantReply(t, reader, "+OK\r\n")

	writeCmd(t, conn, "SETBIT", "str", "1", "1")
	wantReply(t, reader, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

}

func TestTx(t *testing.T) {
	t.Parallel()

	conn, reader := newTestClient(t)

	writeCmd(t, conn, "MULTI")
	wantReply(t, reader, "+OK\r\n")

	writeCmd(t, conn, "SET", "a", "1")
	wantReply(t, reader, "+QUEUED\r\n")

	writeCmd(t, conn, "GET", "a")
	wantReply(t, reader, "+QUEUED\r\n")

	writeCmd(t, conn, "EXEC")
	wantReply(t, reader, "*2\r\n+OK\r\n$1\r\n1\r\n")

	writeCmd(t, conn, "GET", "a")
	wantReply(t, reader, "$1\r\n1\r\n")

	writeCmd(t, conn, "MULTI")
	wantReply(t, reader, "+OK\r\n")

	writeCmd(t, conn, "SET", "b", "2")
	wantReply(t, reader, "+QUEUED\r\n")

	writeCmd(t, conn, "DISCARD")
	wantReply(t, reader, "+OK\r\n")

	writeCmd(t, conn, "GET", "b")
	wantReply(t, reader, "$-1\r\n")
}

func TestWatch(t *testing.T) {
    t.Parallel()

    cfg := config.Config{
        Host:      "127.0.0.1",
        Port:      0,
        LogLevel:  "error",
        Databases: 4,
    }

    eng := engine.New(cfg.Databases)
    t.Cleanup(eng.Close)

    exec := command.NewExecutor(eng)
    srv := server.New(cfg, logger.NewDiscard(), exec)

    ctx, cancel := context.WithCancel(context.Background())
    errCh := make(chan error, 1)
    go func() {
        errCh <- srv.Run(ctx)
    }()

    addr := waitAddr(t, srv)

    conn1, err := net.Dial("tcp", addr)
    if err != nil {
        cancel()
        t.Fatalf("dial first client: %v", err)
    }
    defer conn1.Close()
    reader1 := bufio.NewReader(conn1)

    conn2, err := net.Dial("tcp", addr)
    if err != nil {
        cancel()
        t.Fatalf("dial second client: %v", err)
    }
    defer conn2.Close()
    reader2 := bufio.NewReader(conn2)

    writeCmd(t, conn1, "WATCH", "a")
    wantReply(t, reader1, "+OK\r\n")

    writeCmd(t, conn1, "MULTI")
    wantReply(t, reader1, "+OK\r\n")

    writeCmd(t, conn1, "GET", "a")
    wantReply(t, reader1, "+QUEUED\r\n")

    writeCmd(t, conn2, "SET", "a", "1")
    wantReply(t, reader2, "+OK\r\n")

    writeCmd(t, conn1, "EXEC")
    wantReply(t, reader1, "*-1\r\n")

    cancel()

    select {
    case err := <-errCh:
        if err != nil {
            t.Fatalf("server shutdown: %v", err)
        }
    case <-time.After(2 * time.Second):
        t.Fatal("server did not stop")
    }
}

func TestUnwatch(t *testing.T) {
	t.Parallel()

	cfg := config.Config{
		Host:      "127.0.0.1",
		Port:      0,
		LogLevel:  "error",
		Databases: 4,
	}

	eng := engine.New(cfg.Databases)
	t.Cleanup(eng.Close)

	exec := command.NewExecutor(eng)
	srv := server.New(cfg, logger.NewDiscard(), exec)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Run(ctx)
	}()

	addr := waitAddr(t, srv)

	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		cancel()
		t.Fatalf("dial first client: %v", err)
	}
	defer conn1.Close()
	reader1 := bufio.NewReader(conn1)

	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		cancel()
		t.Fatalf("dial second client: %v", err)
	}
	defer conn2.Close()
	reader2 := bufio.NewReader(conn2)

	writeCmd(t, conn1, "WATCH", "a")
	wantReply(t, reader1, "+OK\r\n")

	writeCmd(t, conn1, "UNWATCH")
	wantReply(t, reader1, "+OK\r\n")

	writeCmd(t, conn1, "MULTI")
	wantReply(t, reader1, "+OK\r\n")

	writeCmd(t, conn1, "GET", "a")
	wantReply(t, reader1, "+QUEUED\r\n")

	writeCmd(t, conn2, "SET", "a", "1")
	wantReply(t, reader2, "+OK\r\n")

	writeCmd(t, conn1, "EXEC")
	wantReply(t, reader1, "*1\r\n$1\r\n1\r\n")

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("server shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop")
	}
}

func TestPubSub(t *testing.T) {
    t.Parallel()

    cfg := config.Config{
        Host:      "127.0.0.1",
        Port:      0,
        LogLevel:  "error",
        Databases: 4,
    }

    eng := engine.New(cfg.Databases)
    t.Cleanup(eng.Close)

    exec := command.NewExecutor(eng)
    srv := server.New(cfg, logger.NewDiscard(), exec)

    ctx, cancel := context.WithCancel(context.Background())
    errCh := make(chan error, 1)
    go func() {
        errCh <- srv.Run(ctx)
    }()

    addr := waitAddr(t, srv)

    subConn, err := net.Dial("tcp", addr)
    if err != nil {
        cancel()
        t.Fatalf("dial subscriber: %v", err)
    }
    defer subConn.Close()
    subReader := bufio.NewReader(subConn)

    pubConn, err := net.Dial("tcp", addr)
    if err != nil {
        cancel()
        t.Fatalf("dial publisher: %v", err)
    }
    defer pubConn.Close()
    pubReader := bufio.NewReader(pubConn)

    writeCmd(t, subConn, "SUBSCRIBE", "news")
    wantReply(t, subReader, "*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n")

    writeCmd(t, pubConn, "PUBLISH", "news", "hello")
    wantReply(t, pubReader, ":1\r\n")
    wantReply(t, subReader, "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n")

    writeCmd(t, subConn, "UNSUBSCRIBE", "news")
    wantReply(t, subReader, "*3\r\n$11\r\nunsubscribe\r\n$4\r\nnews\r\n:0\r\n")

    writeCmd(t, pubConn, "PUBLISH", "news", "again")
    wantReply(t, pubReader, ":0\r\n")

    cancel()

    select {
    case err := <-errCh:
        if err != nil {
            t.Fatalf("server shutdown: %v", err)
        }
    case <-time.After(2 * time.Second):
        t.Fatal("server did not stop")
    }
}

func TestUnsubscribeAll(t *testing.T) {
    t.Parallel()

    cfg := config.Config{
        Host:      "127.0.0.1",
        Port:      0,
        LogLevel:  "error",
        Databases: 4,
    }

    eng := engine.New(cfg.Databases)
    t.Cleanup(eng.Close)

    exec := command.NewExecutor(eng)
    srv := server.New(cfg, logger.NewDiscard(), exec)

    ctx, cancel := context.WithCancel(context.Background())
    errCh := make(chan error, 1)
    go func() {
        errCh <- srv.Run(ctx)
    }()

    addr := waitAddr(t, srv)

    conn, err := net.Dial("tcp", addr)
    if err != nil {
        cancel()
        t.Fatalf("dial subscriber: %v", err)
    }
    defer conn.Close()
    reader := bufio.NewReader(conn)

    writeCmd(t, conn, "SUBSCRIBE", "a", "b")
    wantReply(t, reader, "*3\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:1\r\n")
    wantReply(t, reader, "*3\r\n$9\r\nsubscribe\r\n$1\r\nb\r\n:2\r\n")

    writeCmd(t, conn, "UNSUBSCRIBE")
    wantReply(t, reader, "*3\r\n$11\r\nunsubscribe\r\n$1\r\na\r\n:1\r\n")
    wantReply(t, reader, "*3\r\n$11\r\nunsubscribe\r\n$1\r\nb\r\n:0\r\n")

    cancel()

    select {
    case err := <-errCh:
        if err != nil {
            t.Fatalf("server shutdown: %v", err)
        }
    case <-time.After(2 * time.Second):
        t.Fatal("server did not stop")
    }
}