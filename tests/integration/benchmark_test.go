package integration

import (
	"bufio"
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"godis/internal/command"
	"godis/internal/common/logger"
	"godis/internal/config"
	"godis/internal/engine"
	"godis/internal/server"
)

func BenchmarkServerPing(b *testing.B) {
	addr, shutdown := startBenchmarkServer(b)
	defer shutdown()

	conn, reader := newBenchClient(b, addr)
	defer conn.Close()

	payload := []byte("*1\r\n$4\r\nPING\r\n")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := conn.Write(payload); err != nil {
			b.Fatalf("write ping: %v", err)
		}
		if got := readBenchResp(b, reader); got != "+PONG\r\n" {
			b.Fatalf("unexpected ping reply: %q", got)
		}
	}
}

func BenchmarkServerSetGetParallel(b *testing.B) {
	addr, shutdown := startBenchmarkServer(b)
	defer shutdown()

	var clientID atomic.Uint64

	b.ReportAllocs()
	b.SetParallelism(4)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		conn, reader := newBenchClient(b, addr)
		defer conn.Close()

		id := clientID.Add(1)
		iter := 0

		for pb.Next() {
			key := "bench:" + strconv.FormatUint(id, 10) + ":" + strconv.Itoa(iter)
			value := strconv.Itoa(iter)

			writeBenchCmd(b, conn, "SET", key, value)
			if got := readBenchResp(b, reader); got != "+OK\r\n" {
				b.Fatalf("unexpected SET reply: %q", got)
			}

			writeBenchCmd(b, conn, "GET", key)
			if got := readBenchResp(b, reader); got != bulkReply(value) {
				b.Fatalf("unexpected GET reply: %q", got)
			}

			iter++
		}
	})
}

func startBenchmarkServer(b *testing.B) (string, func()) {
	b.Helper()

	cfg := config.Config{
		Host:      "127.0.0.1",
		Port:      0,
		LogLevel:  "error",
		Databases: 4,
	}

	eng := engine.New(cfg.Databases)
	exec := command.NewExecutor(eng)
	srv := server.New(cfg, logger.NewDiscard(), exec)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Run(ctx)
	}()

	addr := waitBenchAddr(b, srv)

	shutdown := func() {
		cancel()

		select {
		case err := <-errCh:
			if err != nil {
				b.Fatalf("server shutdown: %v", err)
			}
		case <-time.After(2 * time.Second):
			b.Fatal("server did not stop")
		}

		eng.Close()
	}

	return addr, shutdown
}

func waitBenchAddr(b *testing.B, srv *server.Server) string {
	b.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if addr := srv.Addr(); addr != "" {
			return addr
		}
		time.Sleep(10 * time.Millisecond)
	}

	b.Fatal("server did not expose listen address in time")
	return ""
}

func newBenchClient(b *testing.B, addr string) (net.Conn, *bufio.Reader) {
	b.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		b.Fatalf("dial server: %v", err)
	}

	return conn, bufio.NewReader(conn)
}

func writeBenchCmd(b *testing.B, conn net.Conn, parts ...string) {
	b.Helper()

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
		b.Fatalf("write command: %v", err)
	}
}

func readBenchResp(b *testing.B, reader *bufio.Reader) string {
	b.Helper()

	line, err := reader.ReadString('\n')
	if err != nil {
		b.Fatalf("read reply line: %v", err)
	}

	if len(line) == 0 {
		b.Fatal("empty reply")
	}

	if line[0] == '*' {
		size, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(line, "*"), "\r\n"))
		if err != nil {
			b.Fatalf("parse array reply: %v", err)
		}

		var builder strings.Builder
		builder.WriteString(line)
		for i := 0; i < size; i++ {
			builder.WriteString(readBenchResp(b, reader))
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

	size, err := strconv.Atoi(strings.TrimPrefix(sizeLine, "$"))
	if err != nil {
		b.Fatalf("parse bulk size: %v", err)
	}

	body := make([]byte, size+2)
	if _, err := io.ReadFull(reader, body); err != nil {
		b.Fatalf("read bulk body: %v", err)
	}

	return line + string(body)
}

func bulkReply(value string) string {
	return "$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"
}
