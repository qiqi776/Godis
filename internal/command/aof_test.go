package command

import (
	"os"
	"path/filepath"
	"testing"

	"godis/internal/engine"
)

func TestAOFAppendEncodesDBSwitch(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "appendonly.aof")
	log, err := OpenAOF(path)
	if err != nil {
		t.Fatalf("open aof: %v", err)
	}
	defer log.Close()

	if err := log.Append(0, [][]byte{
		[]byte("SET"),
		[]byte("a"),
		[]byte("1"),
	}); err != nil {
		t.Fatalf("append first command: %v", err)
	}

	if err := log.Append(1, [][]byte{
		[]byte("SET"),
		[]byte("b"),
		[]byte("2"),
	}); err != nil {
		t.Fatalf("append second command: %v", err)
	}

	if err := log.Close(); err != nil {
		t.Fatalf("close aof: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read aof: %v", err)
	}

	want := "" +
		"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n" +
		"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n" +
		"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n" +
		"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n"
	if string(data) != want {
		t.Fatalf("unexpected aof content\nwant: %q\ngot:  %q", want, string(data))
	}
}

func TestAOFReplayRestoresState(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "appendonly.aof")

	eng1 := engine.New(2)
	exec1 := NewExecutor(eng1)
	log, err := OpenAOF(path)
	if err != nil {
		t.Fatalf("open aof: %v", err)
	}
	exec1.SetAppender(log)

	sess := &testSession{}

	if got := string(exec1.Execute(sess, cmd("SET", "a", "1"))); got != "+OK\r\n" {
		t.Fatalf("unexpected SET reply: %q", got)
	}
	if got := string(exec1.Execute(sess, cmd("SELECT", "1"))); got != "+OK\r\n" {
		t.Fatalf("unexpected SELECT reply: %q", got)
	}
	if got := string(exec1.Execute(sess, cmd("LPUSH", "list", "x"))); got != ":1\r\n" {
		t.Fatalf("unexpected LPUSH reply: %q", got)
	}

	if err := log.Close(); err != nil {
		t.Fatalf("close aof: %v", err)
	}

	eng2 := engine.New(2)
	exec2 := NewExecutor(eng2)
	replay, err := OpenAOF(path)
	if err != nil {
		t.Fatalf("re-open aof: %v", err)
	}
	defer replay.Close()

	if err := replay.Replay(exec2); err != nil {
		t.Fatalf("replay aof: %v", err)
	}

	db0 := eng2.DB(0)
	value, ok, err := db0.Get("a")
	if err != nil {
		t.Fatalf("get replayed string: %v", err)
	}
	if !ok || string(value) != "1" {
		t.Fatalf("unexpected replayed string: %q ok=%v", string(value), ok)
	}

	db1 := eng2.DB(1)
	values, err := db1.LRange("list", 0, -1)
	if err != nil {
		t.Fatalf("lrange replayed list: %v", err)
	}
	if len(values) != 1 || string(values[0]) != "x" {
		t.Fatalf("unexpected replayed list: %#v", values)
	}
}
