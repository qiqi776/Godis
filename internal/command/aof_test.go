package command

import (
	"os"
	"path/filepath"
	"testing"
	"time"

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

func TestParseFsyncPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		want    FsyncPolicy
		wantErr bool
	}{
		{name: "default", value: "", want: FsyncEverySec},
		{name: "always", value: "always", want: FsyncAlways},
		{name: "everysec", value: "everysec", want: FsyncEverySec},
		{name: "no", value: "no", want: FsyncNo},
		{name: "case insensitive", value: "ALWAYS", want: FsyncAlways},
		{name: "invalid", value: "sometimes", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseFsyncPolicy(tt.value)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("unexpected policy: got=%q want=%q", got, tt.want)
			}
		})
	}
}

func TestAOFFsyncAlways(t *testing.T) {
	t.Parallel()

	log, err := OpenAOF(filepath.Join(t.TempDir(), "appendonly.aof"), FsyncAlways)
	if err != nil {
		t.Fatalf("open aof: %v", err)
	}
	defer log.Close()

	var syncs int
	log.syncFile = func() error {
		syncs++
		return nil
	}

	if err := log.Append(0, cmd("SET", "a", "1")); err != nil {
		t.Fatalf("append first command: %v", err)
	}
	if err := log.Append(0, cmd("SET", "b", "2")); err != nil {
		t.Fatalf("append second command: %v", err)
	}

	if syncs != 2 {
		t.Fatalf("unexpected sync count: %d", syncs)
	}
}

func TestAOFFsyncEverySec(t *testing.T) {
	t.Parallel()

	log, err := OpenAOF(filepath.Join(t.TempDir(), "appendonly.aof"), FsyncEverySec)
	if err != nil {
		t.Fatalf("open aof: %v", err)
	}
	defer log.Close()

	now := time.Unix(100, 0)
	log.now = func() time.Time {
		return now
	}

	var syncs int
	log.syncFile = func() error {
		syncs++
		return nil
	}

	if err := log.Append(0, cmd("SET", "a", "1")); err != nil {
		t.Fatalf("append first command: %v", err)
	}
	if err := log.Append(0, cmd("SET", "b", "2")); err != nil {
		t.Fatalf("append second command: %v", err)
	}
	if syncs != 1 {
		t.Fatalf("unexpected sync count before interval: %d", syncs)
	}

	now = now.Add(time.Second)
	if err := log.Append(0, cmd("SET", "c", "3")); err != nil {
		t.Fatalf("append third command: %v", err)
	}
	if syncs != 2 {
		t.Fatalf("unexpected sync count after interval: %d", syncs)
	}
}

func TestAOFFsyncNo(t *testing.T) {
	t.Parallel()

	log, err := OpenAOF(filepath.Join(t.TempDir(), "appendonly.aof"), FsyncNo)
	if err != nil {
		t.Fatalf("open aof: %v", err)
	}
	defer log.Close()

	var syncs int
	log.syncFile = func() error {
		syncs++
		return nil
	}

	if err := log.Append(0, cmd("SET", "a", "1")); err != nil {
		t.Fatalf("append command: %v", err)
	}
	if syncs != 0 {
		t.Fatalf("unexpected sync count: %d", syncs)
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

func TestAOFRewriteCompactsFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "appendonly.aof")
	log, err := OpenAOF(path, FsyncAlways)
	if err != nil {
		t.Fatalf("open aof: %v", err)
	}
	defer log.Close()

	if err := log.Append(0, cmd("SET", "a", "1")); err != nil {
		t.Fatalf("append first command: %v", err)
	}
	if err := log.Append(0, cmd("SET", "a", "2")); err != nil {
		t.Fatalf("append second command: %v", err)
	}

	if err := log.Rewrite(func() [][][]byte {
		return [][][]byte{
			cmd("SELECT", "0"),
			cmd("SET", "a", "2"),
		}
	}); err != nil {
		t.Fatalf("rewrite aof: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read rewritten aof: %v", err)
	}

	want := "" +
		"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n" +
		"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n2\r\n"
	if string(data) != want {
		t.Fatalf("unexpected rewritten aof\nwant: %q\ngot:  %q", want, string(data))
	}

	if err := log.Append(0, cmd("SET", "b", "3")); err != nil {
		t.Fatalf("append after rewrite: %v", err)
	}

	if err := log.Close(); err != nil {
		t.Fatalf("close aof: %v", err)
	}

	eng := engine.New(1)
	exec := NewExecutor(eng)
	replay, err := OpenAOF(path)
	if err != nil {
		t.Fatalf("re-open aof: %v", err)
	}
	defer replay.Close()

	if err := replay.Replay(exec); err != nil {
		t.Fatalf("replay rewritten aof: %v", err)
	}

	value, ok, err := eng.DB(0).Get("a")
	if err != nil {
		t.Fatalf("get a: %v", err)
	}
	if !ok || string(value) != "2" {
		t.Fatalf("unexpected value for a: %q ok=%v", string(value), ok)
	}

	value, ok, err = eng.DB(0).Get("b")
	if err != nil {
		t.Fatalf("get b: %v", err)
	}
	if !ok || string(value) != "3" {
		t.Fatalf("unexpected value for b: %q ok=%v", string(value), ok)
	}
}

func TestBGRewriteAOF(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "appendonly.aof")

	eng := engine.New(1)
	exec := NewExecutor(eng)
	log, err := OpenAOF(path, FsyncAlways)
	if err != nil {
		t.Fatalf("open aof: %v", err)
	}
	defer log.Close()

	exec.SetAppender(log)
	exec.SetRewriter(log)

	sess := &testSession{}
	exec.Execute(sess, cmd("SET", "a", "1"))
	exec.Execute(sess, cmd("SET", "a", "2"))
	exec.Execute(sess, cmd("DEL", "missing"))

	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read before rewrite: %v", err)
	}

	if got := string(exec.Execute(sess, cmd("BGREWRITEAOF"))); got != "+OK\r\n" {
		t.Fatalf("unexpected BGREWRITEAOF reply: %q", got)
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read after rewrite: %v", err)
	}
	if len(after) >= len(before) {
		t.Fatalf("expected rewrite to compact file, before=%d after=%d", len(before), len(after))
	}

	eng2 := engine.New(1)
	exec2 := NewExecutor(eng2)
	replay, err := OpenAOF(path)
	if err != nil {
		t.Fatalf("re-open aof: %v", err)
	}
	defer replay.Close()

	if err := replay.Replay(exec2); err != nil {
		t.Fatalf("replay rewritten aof: %v", err)
	}

	value, ok, err := eng2.DB(0).Get("a")
	if err != nil {
		t.Fatalf("get a after replay: %v", err)
	}
	if !ok || string(value) != "2" {
		t.Fatalf("unexpected replayed value: %q ok=%v", string(value), ok)
	}
}

func TestBGRewriteAOFDisabled(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(1))
	sess := &testSession{}

	if got := string(exec.Execute(sess, cmd("BGREWRITEAOF"))); got != "-ERR AOF is not enabled\r\n" {
		t.Fatalf("unexpected BGREWRITEAOF disabled reply: %q", got)
	}
}
