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

func TestServerPingAndBasicKV(t *testing.T) {
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

	addr := waitForAddr(t, srv)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		cancel()
		t.Fatalf("dial server: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	writeRESP(t, conn, "PING")
	assertReply(t, reader, "+PONG\r\n")

	writeRESP(t, conn, "SET", "demo", "42")
	assertReply(t, reader, "+OK\r\n")

	writeRESP(t, conn, "GET", "demo")
	assertReply(t, reader, "$2\r\n42\r\n")

	writeRESP(t, conn, "EXISTS", "demo")
	assertReply(t, reader, ":1\r\n")

	writeRESP(t, conn, "DEL", "demo")
	assertReply(t, reader, ":1\r\n")

	writeRESP(t, conn, "GET", "demo")
	assertReply(t, reader, "$-1\r\n")

	writeRESP(t, conn, "NOPE")
	assertReply(t, reader, "-ERR unknown command 'nope'\r\n")

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

func TestServerTTLCommands(t *testing.T) {
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

	addr := waitForAddr(t, srv)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		cancel()
		t.Fatalf("dial server: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	writeRESP(t, conn, "SET", "temp", "42")
	assertReply(t, reader, "+OK\r\n")

	writeRESP(t, conn, "TTL", "temp")
	assertReply(t, reader, ":-1\r\n")

	writeRESP(t, conn, "EXPIRE", "temp", "2")
	assertReply(t, reader, ":1\r\n")

	writeRESP(t, conn, "TTL", "temp")
	assertIntegerReplyInRange(t, reader, 1, 2)

	writeRESP(t, conn, "PERSIST", "temp")
	assertReply(t, reader, ":1\r\n")

	writeRESP(t, conn, "TTL", "temp")
	assertReply(t, reader, ":-1\r\n")

	writeRESP(t, conn, "EXPIRE", "temp", "1")
	assertReply(t, reader, ":1\r\n")

	time.Sleep(1100 * time.Millisecond)

	writeRESP(t, conn, "GET", "temp")
	assertReply(t, reader, "$-1\r\n")

	writeRESP(t, conn, "TTL", "temp")
	assertReply(t, reader, ":-2\r\n")

	writeRESP(t, conn, "PERSIST", "missing")
	assertReply(t, reader, ":0\r\n")

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

func waitForAddr(t *testing.T, srv *server.Server) string {
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

func writeRESP(t *testing.T, conn net.Conn, parts ...string) {
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

func assertReply(t *testing.T, reader *bufio.Reader, want string) {
	t.Helper()

	got := readReply(t, reader)
	if got != want {
		t.Fatalf("unexpected reply\nwant: %q\ngot:  %q", want, got)
	}
}

func assertIntegerReplyInRange(t *testing.T, reader *bufio.Reader, min, max int64) {
	t.Helper()

	got := readReply(t, reader)
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

func readReply(t *testing.T, reader *bufio.Reader) string {
	t.Helper()

	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read reply line: %v", err)
	}

	if len(line) == 0 {
		t.Fatal("empty reply")
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
