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

    eng := engine.NewEngine(cfg.Databases)
    t.Cleanup(eng.Close)

    exec := command.NewExecutor(eng)
    srv := server.NewServer(cfg, logger.NewDiscard(), exec)

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