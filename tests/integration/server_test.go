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

func TestKV(t *testing.T) {
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
		t.Fatalf("dial server: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

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

func TestTTL(t *testing.T) {
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
		t.Fatalf("dial server: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

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

func TestSelect(t *testing.T) {
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
		t.Fatalf("dial server: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

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
