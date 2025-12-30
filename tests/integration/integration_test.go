package integration

import (
	"bufio"
	"fmt"
	"godis/internal/commands"
	"godis/internal/config"
	"godis/internal/database"
	"godis/internal/tcp"
	"godis/pkg/logger"
	"net"
	"strings"
	"testing"
	"time"
)

func setupServer() (string, func()) {
	logger.Init("error", "")
	commands.Init()
	port := "6399"
	cfg := &config.Config{
		Port:        port,
		LogLevel:    "error",
		AppendOnly:  false,
	}
	db := database.NewStandalone()
	db.StartCleanTask()
	server := tcp.NewServer(cfg, db)
	go server.Start()
	time.Sleep(200 * time.Millisecond)
	addr := "127.0.0.1:" + port
	teardown := func() {
		server.Stop()
		db.Close()
	}
	return addr, teardown
}

func sendCommand(t *testing.T, conn net.Conn, cmd string) string {
	t.Helper()
	parts := strings.Split(cmd, " ")
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(parts)))
	for _, part := range parts {
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(part), part))
	}
	_, err := conn.Write([]byte(sb.String()))
	if err != nil {
		t.Fatalf("Failed to write command '%s': %v", cmd, err)
	}
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read response header for '%s': %v", cmd, err)
	}
	if strings.HasPrefix(line, "$") {
		if strings.TrimSpace(line) == "$-1" {
			return line
		}
		line2, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read bulk string body: %v", err)
		}
		return line + line2
	}
	return line
}

func TestGodisIntegration(t *testing.T) {
	addr, teardown := setupServer()
	defer teardown()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Could not connect to test server: %v", err)
	}
	defer conn.Close()
	t.Run("Basic String", func(t *testing.T) {
		resp := sendCommand(t, conn, "SET name godis")
		if resp != "+OK\r\n" {
			t.Errorf("SET expected +OK, got %q", resp)
		}
		resp = sendCommand(t, conn, "GET name")
		if resp != "$5\r\ngodis\r\n" {
			t.Errorf("GET expected godis, got %q", resp)
		}
		resp = sendCommand(t, conn, "GET not_exist")
		if resp != "$-1\r\n" {
			t.Errorf("GET not_exist expected $-1, got %q", resp)
		}
	})
	t.Run("Expiration", func(t *testing.T) {
		resp := sendCommand(t, conn, "EXPIRE name 100")
		if resp != ":1\r\n" {
			t.Errorf("EXPIRE expected :1, got %q", resp)
		}
		resp = sendCommand(t, conn, "TTL name")
		if strings.HasPrefix(resp, ":-") {
			t.Errorf("TTL expected positive, got %q", resp)
		}
		resp = sendCommand(t, conn, "PERSIST name")
		if resp != ":1\r\n" {
			t.Errorf("PERSIST expected :1, got %q", resp)
		}
		resp = sendCommand(t, conn, "TTL name")
		if resp != ":-1\r\n" {
			t.Errorf("TTL after PERSIST expected :-1, got %q", resp)
		}
	})
	t.Run("Multi-DB Isolation", func(t *testing.T) {
		sendCommand(t, conn, "SET db_test val_0")
		resp := sendCommand(t, conn, "SELECT 1")
		if resp != "+OK\r\n" {
			t.Errorf("SELECT 1 failed, got %q", resp)
		}
		resp = sendCommand(t, conn, "GET db_test")
		if resp != "$-1\r\n" {
			t.Errorf("Isolation failed: Key from DB0 leaked to DB1. Got %q", resp)
		}
		sendCommand(t, conn, "SET db_test val_1")
		sendCommand(t, conn, "SELECT 0")
		resp = sendCommand(t, conn, "GET db_test")
		if resp != "$5\r\nval_0\r\n" {
			t.Errorf("Switch back failed: Expected val_0, got %q", resp)
		}
	})
	t.Run("Hash Operations", func(t *testing.T) {
		resp := sendCommand(t, conn, "HSET user:1 name bob age 20")
		if resp != ":2\r\n" {
			t.Errorf("HSET expected :2, got %q", resp)
		}
		resp = sendCommand(t, conn, "HGET user:1 name")
		if resp != "$3\r\nbob\r\n" {
			t.Errorf("HGET expected bob, got %q", resp)
		}
	})
	t.Run("Lazy Deletion", func(t *testing.T) {
		sendCommand(t, conn, "SET lazy_key val")
		sendCommand(t, conn, "EXPIRE lazy_key 1")
		time.Sleep(1200 * time.Millisecond)
		resp := sendCommand(t, conn, "GET lazy_key")
		if resp != "$-1\r\n" {
			t.Errorf("Lazy deletion failed: Expected nil, got %q", resp)
		}
		resp = sendCommand(t, conn, "EXISTS lazy_key")
		if resp != ":0\r\n" {
			t.Errorf("Key should create not exist, got %q", resp)
		}
	})
}