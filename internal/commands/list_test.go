package commands

import (
	"godis/internal/core"
	"godis/internal/database"
	"godis/pkg/protocol"
	"testing"
)

func TestLPushLPop(t *testing.T) {
	db := database.NewStandalone()
	ctx := &core.Context{
		DB:   db,
		Conn: core.NewConnection(),
	}
	ctx.Args = makeArgs("key", "a", "b", "c")
	res := LPush(ctx)
	if res.Num != 3 {
		t.Errorf("LPush expected 3, got %d", res.Num)
	}
	ctx.Args = makeArgs("key")
	res = LPop(ctx)
	if string(res.Bulk) != "c" {
		t.Errorf("LPop expected 'c', got %s", res.Bulk)
	}
	res = LLen(ctx)
	if res.Num != 2 {
		t.Errorf("LLen expected 2, got %d", res.Num)
	}
}

func TestLRange(t *testing.T) {
	db := database.NewStandalone()
	ctx := &core.Context{
		DB:   db,
		Conn: core.NewConnection(),
	}
	ctx.Args = makeArgs("key", "1", "2", "3")
	RPush(ctx)
	ctx.Args = makeArgs("key", "0", "-1")
	res := LRange(ctx)
	if len(res.Array) != 3 {
		t.Fatalf("LRange expected length 3, got %d", len(res.Array))
	}
	if string(res.Array[0].Bulk) != "1" || string(res.Array[2].Bulk) != "3" {
		t.Error("LRange content mismatch")
	}
}

func makeArgs(args ...string) []protocol.Value {
	res := make([]protocol.Value, len(args))
	for i, v := range args {
		res[i] = protocol.Value{Type: protocol.BulkString, Bulk: []byte(v)}
	}
	return res
}