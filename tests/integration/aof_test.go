package integration

import (
	"bufio"
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"godis/internal/app"
)

func TestAOFRecovery(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "godis.yaml")
	aofPath := filepath.Join(tmpDir, "appendonly.aof")

	cfg := "" +
		"host: 127.0.0.1\n" +
		"port: 0\n" +
		"log_level: error\n" +
		"databases: 4\n" +
		"aof_enabled: true\n" +
		"aof_path: " + aofPath + "\n" +
		"aof_fsync: always\n"
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	app1, err := app.Bootstrap(cfgPath)
	if err != nil {
		t.Fatalf("bootstrap first app: %v", err)
	}
	errCh1, cancel1 := runApp(t, app1)

	conn1, err := net.Dial("tcp", waitAddr(t, app1.Server))
	if err != nil {
		cancel1()
		t.Fatalf("dial first app: %v", err)
	}
	reader1 := bufio.NewReader(conn1)

	writeCmd(t, conn1, "SET", "a", "1")
	wantReply(t, reader1, "+OK\r\n")

	writeCmd(t, conn1, "SELECT", "1")
	wantReply(t, reader1, "+OK\r\n")

	writeCmd(t, conn1, "HSET", "user", "name", "godis")
	wantReply(t, reader1, ":1\r\n")

	_ = conn1.Close()
	stopApp(t, cancel1, errCh1)

	app2, err := app.Bootstrap(cfgPath)
	if err != nil {
		t.Fatalf("bootstrap second app: %v", err)
	}
	errCh2, cancel2 := runApp(t, app2)
	defer stopApp(t, cancel2, errCh2)

	conn2, err := net.Dial("tcp", waitAddr(t, app2.Server))
	if err != nil {
		cancel2()
		t.Fatalf("dial second app: %v", err)
	}
	defer conn2.Close()
	reader2 := bufio.NewReader(conn2)

	writeCmd(t, conn2, "GET", "a")
	wantReply(t, reader2, "$1\r\n1\r\n")

	writeCmd(t, conn2, "SELECT", "1")
	wantReply(t, reader2, "+OK\r\n")

	writeCmd(t, conn2, "HGET", "user", "name")
	wantReply(t, reader2, "$5\r\ngodis\r\n")
}

func TestBGRewriteAOFRecovery(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "godis.yaml")
	aofPath := filepath.Join(tmpDir, "appendonly.aof")

	cfg := "" +
		"host: 127.0.0.1\n" +
		"port: 0\n" +
		"log_level: error\n" +
		"databases: 4\n" +
		"aof_enabled: true\n" +
		"aof_path: " + aofPath + "\n" +
		"aof_fsync: always\n"
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	app1, err := app.Bootstrap(cfgPath)
	if err != nil {
		t.Fatalf("bootstrap first app: %v", err)
	}
	errCh1, cancel1 := runApp(t, app1)

	conn1, err := net.Dial("tcp", waitAddr(t, app1.Server))
	if err != nil {
		cancel1()
		t.Fatalf("dial first app: %v", err)
	}
	reader1 := bufio.NewReader(conn1)

	writeCmd(t, conn1, "SET", "a", "1")
	wantReply(t, reader1, "+OK\r\n")

	writeCmd(t, conn1, "SET", "a", "2")
	wantReply(t, reader1, "+OK\r\n")

	writeCmd(t, conn1, "BGREWRITEAOF")
	wantReply(t, reader1, "+OK\r\n")

	_ = conn1.Close()
	stopApp(t, cancel1, errCh1)

	app2, err := app.Bootstrap(cfgPath)
	if err != nil {
		t.Fatalf("bootstrap second app: %v", err)
	}
	errCh2, cancel2 := runApp(t, app2)
	defer stopApp(t, cancel2, errCh2)

	conn2, err := net.Dial("tcp", waitAddr(t, app2.Server))
	if err != nil {
		cancel2()
		t.Fatalf("dial second app: %v", err)
	}
	defer conn2.Close()
	reader2 := bufio.NewReader(conn2)

	writeCmd(t, conn2, "GET", "a")
	wantReply(t, reader2, "$1\r\n2\r\n")
}

func runApp(t *testing.T, application *app.App) (chan error, context.CancelFunc) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- application.Run(ctx)
	}()

	return errCh, cancel
}

func stopApp(t *testing.T, cancel context.CancelFunc, errCh chan error) {
	t.Helper()

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("app shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("app did not stop")
	}
}
