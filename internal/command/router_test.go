package command

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"godis/internal/engine"
)

type testSession struct {
	dbIndex int
}

func (s *testSession) GetDBIndex() int {
	return s.dbIndex
}

func (s *testSession) SetDBIndex(index int) {
	s.dbIndex = index
}

func TestPing(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	got := string(exec.Execute(sess, cmd("PING")))
	want := "+PONG\r\n"
	if got != want {
		t.Fatalf("want %q, got %q", want, got)
	}
}

func TestPingEcho(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	got := string(exec.Execute(sess, cmd("PING", "hello")))
	want := "$5\r\nhello\r\n"
	if got != want {
		t.Fatalf("want %q, got %q", want, got)
	}
}

func TestSetGet(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	if got := string(exec.Execute(sess, cmd("SET", "a", "1"))); got != "+OK\r\n" {
		t.Fatalf("unexpected SET reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("GET", "a"))); got != "$1\r\n1\r\n" {
		t.Fatalf("unexpected GET reply: %q", got)
	}
}

func TestDelExists(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	exec.Execute(sess, cmd("SET", "a", "1"))
	exec.Execute(sess, cmd("SET", "b", "2"))

	if got := string(exec.Execute(sess, cmd("EXISTS", "a", "b", "c"))); got != ":2\r\n" {
		t.Fatalf("unexpected EXISTS reply: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("DEL", "a", "b"))); got != ":2\r\n" {
		t.Fatalf("unexpected DEL reply: %q", got)
	}
}

func TestTTL(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	exec.Execute(sess, cmd("SET", "k", "v"))

	if got := string(exec.Execute(sess, cmd("TTL", "k"))); got != ":-1\r\n" {
		t.Fatalf("unexpected TTL reply before expire: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("EXPIRE", "k", "1"))); got != ":1\r\n" {
		t.Fatalf("unexpected EXPIRE reply: %q", got)
	}

	raw := strings.TrimSuffix(strings.TrimPrefix(string(exec.Execute(sess, cmd("TTL", "k"))), ":"), "\r\n")
	ttl, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse TTL reply: %v", err)
	}
	if ttl < 1 {
		t.Fatalf("expected positive TTL, got %d", ttl)
	}

	time.Sleep(1100 * time.Millisecond)

	if got := string(exec.Execute(sess, cmd("TTL", "k"))); got != ":-2\r\n" {
		t.Fatalf("unexpected TTL reply after expire: %q", got)
	}
}

func TestSelect(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	exec.Execute(sess, cmd("SET", "a", "1"))

	if got := string(exec.Execute(sess, cmd("SELECT", "1"))); got != "+OK\r\n" {
		t.Fatalf("unexpected SELECT reply: %q", got)
	}
	if sess.dbIndex != 1 {
		t.Fatalf("expected db index 1, got %d", sess.dbIndex)
	}

	if got := string(exec.Execute(sess, cmd("GET", "a"))); got != "$-1\r\n" {
		t.Fatalf("unexpected GET reply in db1: %q", got)
	}

	exec.Execute(sess, cmd("SET", "a", "2"))
	exec.Execute(sess, cmd("SELECT", "0"))

	if got := string(exec.Execute(sess, cmd("GET", "a"))); got != "$1\r\n1\r\n" {
		t.Fatalf("unexpected GET reply in db0: %q", got)
	}
}

func TestSelectErr(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	if got := string(exec.Execute(sess, cmd("SELECT", "x"))); got != "-ERR value is not an integer or out of range\r\n" {
		t.Fatalf("unexpected SELECT type error: %q", got)
	}

	if got := string(exec.Execute(sess, cmd("SELECT", "9"))); got != "-ERR DB index is out of range\r\n" {
		t.Fatalf("unexpected SELECT range error: %q", got)
	}
}

func TestUnknown(t *testing.T) {
	t.Parallel()

	exec := NewExecutor(engine.New(2))
	sess := &testSession{}

	got := string(exec.Execute(sess, cmd("NOPE")))
	want := "-ERR unknown command 'nope'\r\n"
	if got != want {
		t.Fatalf("want %q, got %q", want, got)
	}
}

func cmd(parts ...string) [][]byte {
	out := make([][]byte, 0, len(parts))
	for _, part := range parts {
		out = append(out, []byte(part))
	}
	return out
}
