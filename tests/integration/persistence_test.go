package integration

import (
	"bufio"
	"net"
	"os"
	"path/filepath"
	"testing"

	"godis/internal/app"
)

func TestRDBBootstrapRecovery(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "godis.yaml")
	rdbPath := filepath.Join(tmpDir, "dump.rdb")

	cfg := "" +
		"host: 127.0.0.1\n" +
		"port: 0\n" +
		"log_level: error\n" +
		"databases: 4\n" +
		"rdb_enabled: true\n" +
		"rdb_path: " + rdbPath + "\n"
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

	writeCmd(t, conn1, "EXPIRE", "a", "60")
	wantReply(t, reader1, ":1\r\n")

	writeCmd(t, conn1, "RPUSH", "list", "x", "y")
	wantReply(t, reader1, ":2\r\n")

	writeCmd(t, conn1, "SELECT", "1")
	wantReply(t, reader1, "+OK\r\n")

	writeCmd(t, conn1, "HSET", "user", "name", "godis")
	wantReply(t, reader1, ":1\r\n")

	writeCmd(t, conn1, "SAVE")
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
	wantReply(t, reader2, "$1\r\n1\r\n")

	writeCmd(t, conn2, "TTL", "a")
	wantInt(t, reader2, 1, 60)

	writeCmd(t, conn2, "LRANGE", "list", "0", "-1")
	wantReply(t, reader2, "*2\r\n$1\r\nx\r\n$1\r\ny\r\n")

	writeCmd(t, conn2, "SELECT", "1")
	wantReply(t, reader2, "+OK\r\n")

	writeCmd(t, conn2, "HGET", "user", "name")
	wantReply(t, reader2, "$5\r\ngodis\r\n")
}

func TestHybridAOFBootstrapRecovery(t *testing.T) {
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
		"aof_fsync: always\n" +
		"aof_use_rdb_preamble: true\n"
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

	writeCmd(t, conn1, "SET", "before", "rewrite")
	wantReply(t, reader1, "+OK\r\n")

	writeCmd(t, conn1, "SELECT", "1")
	wantReply(t, reader1, "+OK\r\n")

	writeCmd(t, conn1, "HSET", "user", "name", "godis")
	wantReply(t, reader1, ":1\r\n")

	writeCmd(t, conn1, "BGREWRITEAOF")
	wantReply(t, reader1, "+OK\r\n")

	writeCmd(t, conn1, "SET", "after", "rewrite")
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

	writeCmd(t, conn2, "GET", "before")
	wantReply(t, reader2, "$7\r\nrewrite\r\n")

	writeCmd(t, conn2, "SELECT", "1")
	wantReply(t, reader2, "+OK\r\n")

	writeCmd(t, conn2, "HGET", "user", "name")
	wantReply(t, reader2, "$5\r\ngodis\r\n")

	writeCmd(t, conn2, "GET", "after")
	wantReply(t, reader2, "$7\r\nrewrite\r\n")
}

func TestCorruptRDBBootstrapFails(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "godis.yaml")
	rdbPath := filepath.Join(tmpDir, "dump.rdb")

	if err := os.WriteFile(rdbPath, []byte("GODISRDB1\nbad-gob"), 0o644); err != nil {
		t.Fatalf("write corrupt rdb: %v", err)
	}

	cfg := "" +
		"host: 127.0.0.1\n" +
		"port: 0\n" +
		"log_level: error\n" +
		"databases: 4\n" +
		"rdb_enabled: true\n" +
		"rdb_path: " + rdbPath + "\n"
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if _, err := app.Bootstrap(cfgPath); err == nil {
		t.Fatal("expected corrupt rdb bootstrap to fail")
	}
}

func TestCorruptHybridAOFBootstrapFails(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "godis.yaml")
	aofPath := filepath.Join(tmpDir, "appendonly.aof")

	if err := os.WriteFile(aofPath, []byte("GODISRDB1\nbad-gob"), 0o644); err != nil {
		t.Fatalf("write corrupt aof: %v", err)
	}

	cfg := "" +
		"host: 127.0.0.1\n" +
		"port: 0\n" +
		"log_level: error\n" +
		"databases: 4\n" +
		"aof_enabled: true\n" +
		"aof_path: " + aofPath + "\n" +
		"aof_fsync: always\n" +
		"aof_use_rdb_preamble: true\n"
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if _, err := app.Bootstrap(cfgPath); err == nil {
		t.Fatal("expected corrupt hybrid aof bootstrap to fail")
	}
}
