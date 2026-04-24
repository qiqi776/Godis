package command

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"godis/internal/engine"
)

func TestRDBDumpLoadRestoresState(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "dump.rdb")
	eng1 := engine.New(2)
	db0 := eng1.DB(0)
	db1 := eng1.DB(1)

	db0.Set("a", []byte("1"))
	if ok := db0.Expire("a", time.Minute); !ok {
		t.Fatal("expire a")
	}
	if _, err := db0.RPush("list", []byte("x"), []byte("y")); err != nil {
		t.Fatalf("rpush: %v", err)
	}
	db1.Set("b", []byte("2"))

	rdb, err := NewRDBFile(path)
	if err != nil {
		t.Fatalf("new rdb: %v", err)
	}
	if err := rdb.Dump(eng1.AOFRewriteCommands); err != nil {
		t.Fatalf("dump rdb: %v", err)
	}

	eng2 := engine.New(2)
	exec2 := NewExecutor(eng2)
	if err := rdb.Load(exec2); err != nil {
		t.Fatalf("load rdb: %v", err)
	}

	value, ok, err := eng2.DB(0).Get("a")
	if err != nil || !ok || string(value) != "1" {
		t.Fatalf("unexpected db0 a: value=%q ok=%v err=%v", value, ok, err)
	}
	if ttl := eng2.DB(0).TTL("a"); ttl <= 0 {
		t.Fatalf("expected restored ttl, got %d", ttl)
	}
	values, err := eng2.DB(0).LRange("list", 0, -1)
	if err != nil || len(values) != 2 || string(values[0]) != "x" || string(values[1]) != "y" {
		t.Fatalf("unexpected list: values=%q err=%v", values, err)
	}
	value, ok, err = eng2.DB(1).Get("b")
	if err != nil || !ok || string(value) != "2" {
		t.Fatalf("unexpected db1 b: value=%q ok=%v err=%v", value, ok, err)
	}
}

func TestSaveCommandDumpsRDB(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "dump.rdb")
	eng := engine.New(1)
	exec := NewExecutor(eng)
	rdb, err := NewRDBFile(path)
	if err != nil {
		t.Fatalf("new rdb: %v", err)
	}
	exec.SetDumper(rdb)

	sess := &testSession{}
	if got := string(exec.Execute(sess, cmd("SET", "a", "1"))); got != "+OK\r\n" {
		t.Fatalf("unexpected SET reply: %q", got)
	}
	if got := string(exec.Execute(sess, cmd("SAVE"))); got != "+OK\r\n" {
		t.Fatalf("unexpected SAVE reply: %q", got)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stat rdb: %v", err)
	}
}

func TestHybridAOFRewriteWithRDBPreamble(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "appendonly.aof")
	eng1 := engine.New(2)
	exec1 := NewExecutor(eng1)
	log, err := OpenAOFWithPreamble(path, true, FsyncAlways)
	if err != nil {
		t.Fatalf("open aof: %v", err)
	}
	exec1.SetAppender(log)
	exec1.SetRewriter(log)

	sess := &testSession{}
	if got := string(exec1.Execute(sess, cmd("SET", "a", "1"))); got != "+OK\r\n" {
		t.Fatalf("unexpected SET reply: %q", got)
	}
	if got := string(exec1.Execute(sess, cmd("SELECT", "1"))); got != "+OK\r\n" {
		t.Fatalf("unexpected SELECT reply: %q", got)
	}
	if got := string(exec1.Execute(sess, cmd("HSET", "user", "name", "godis"))); got != ":1\r\n" {
		t.Fatalf("unexpected HSET reply: %q", got)
	}
	if got := string(exec1.Execute(sess, cmd("BGREWRITEAOF"))); got != "+OK\r\n" {
		t.Fatalf("unexpected BGREWRITEAOF reply: %q", got)
	}
	if got := string(exec1.Execute(sess, cmd("SET", "after", "rewrite"))); got != "+OK\r\n" {
		t.Fatalf("unexpected post-rewrite SET reply: %q", got)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close aof: %v", err)
	}

	eng2 := engine.New(2)
	exec2 := NewExecutor(eng2)
	replay, err := OpenAOFWithPreamble(path, true, FsyncAlways)
	if err != nil {
		t.Fatalf("reopen aof: %v", err)
	}
	defer replay.Close()
	if err := replay.Replay(exec2); err != nil {
		t.Fatalf("replay hybrid aof: %v", err)
	}

	value, ok, err := eng2.DB(0).Get("a")
	if err != nil || !ok || string(value) != "1" {
		t.Fatalf("unexpected db0 a: value=%q ok=%v err=%v", value, ok, err)
	}
	value, ok, err = eng2.DB(1).HGet("user", "name")
	if err != nil || !ok || string(value) != "godis" {
		t.Fatalf("unexpected db1 user.name: value=%q ok=%v err=%v", value, ok, err)
	}
	value, ok, err = eng2.DB(1).Get("after")
	if err != nil || !ok || string(value) != "rewrite" {
		t.Fatalf("unexpected db1 after: value=%q ok=%v err=%v", value, ok, err)
	}
}
