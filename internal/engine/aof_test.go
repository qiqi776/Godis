package engine

import (
	"strings"
	"testing"
	"time"
)

func TestAOFRewriteCommands(t *testing.T) {
	t.Parallel()

	eng := New(2)
	db0 := eng.DB(0)
	db1 := eng.DB(1)

	db0.Set("str", []byte("1"))
	if _, err := db0.RPush("list", []byte("a"), []byte("b")); err != nil {
		t.Fatalf("rpush: %v", err)
	}
	if _, err := db0.HSet("hash", "field", []byte("value")); err != nil {
		t.Fatalf("hset: %v", err)
	}
	if _, err := db0.SAdd("set", "member"); err != nil {
		t.Fatalf("sadd: %v", err)
	}
	if _, err := db0.ZAdd("zset", 1.5, "member"); err != nil {
		t.Fatalf("zadd: %v", err)
	}
	if _, err := db0.SetBit("bitmap", 7, 1); err != nil {
		t.Fatalf("setbit: %v", err)
	}
	if ok := db0.Expire("str", time.Minute); !ok {
		t.Fatal("expire")
	}

	db1.Set("other", []byte("2"))

	commands := eng.AOFRewriteCommands()
	got := stringifyCommands(commands)

	if len(commands) == 0 {
		t.Fatal("expected rewrite commands")
	}
	if got[0] != "SELECT 0" {
		t.Fatalf("unexpected first command: %q", got[0])
	}

	required := map[string]bool{
		"SET str 1":             false,
		"EXPIRE str 60":         false,
		"RPUSH list a b":        false,
		"HSET hash field value": false,
		"SADD set member":       false,
		"ZADD zset 1.5 member":  false,
		"SETBIT bitmap 7 1":     false,
		"SELECT 1":              false,
		"SET other 2":           false,
	}

	for _, command := range got {
		if _, ok := required[command]; ok {
			required[command] = true
		}
	}

	for command, seen := range required {
		if !seen {
			t.Fatalf("missing rewrite command %q in %#v", command, got)
		}
	}
}

func stringifyCommands(commands [][][]byte) []string {
	out := make([]string, 0, len(commands))
	for _, command := range commands {
		parts := make([]string, 0, len(command))
		for _, token := range command {
			parts = append(parts, string(token))
		}
		out = append(out, joinParts(parts))
	}
	return out
}

func joinParts(parts []string) string {
	return strings.Join(parts, " ")
}
