package commands

import (
	"godis/internal/core"
	"godis/internal/database"
	"testing"
)

func TestZSetCommands(t *testing.T) {
	db := database.NewStandalone()
	ctx := &core.Context{
		DB:   db,
		Conn: core.NewConnection(),
	}
	ctx.Args = makeArgs("myzset", "10", "a", "20", "b")
	res := ZAdd(ctx)
	if res.Num != 2 {
		t.Errorf("ZADD expected 2, got %d", res.Num)
	}
	ctx.Args = makeArgs("myzset", "a")
	res = ZScore(ctx)
	if string(res.Bulk) != "10" {
		t.Errorf("ZSCORE expected 10, got %s", res.Bulk)
	}
	ctx.Args = makeArgs("myzset", "b")
	res = ZRank(ctx)
	if res.Num != 1 {
		t.Errorf("ZRANK expected 1, got %d", res.Num)
	}
	ctx.Args = makeArgs("myzset", "a")
	res = ZRevRank(ctx)
	if res.Num != 1 {
		t.Errorf("ZREVRANK expected 1, got %d", res.Num)
	}
	ctx.Args = makeArgs("myzset", "0", "-1", "WITHSCORES")
	res = ZRange(ctx)
	if len(res.Array) != 4 {
		t.Fatalf("ZRANGE expected len 4, got %d", len(res.Array))
	}
	if string(res.Array[0].Bulk) != "a" || string(res.Array[1].Bulk) != "10" {
		t.Error("ZRANGE content mismatch")
	}
	ctx.Args = makeArgs("myzset", "a")
	res = ZRem(ctx)
	if res.Num != 1 {
		t.Errorf("ZREM expected 1, got %d", res.Num)
	}
}

func TestUndoZAdd(t *testing.T) {
	db := database.NewStandalone()
	ctx := &core.Context{
		DB:   db,
		Conn: core.NewConnection(),
	}
	ctx.Args = makeArgs("undo_zset", "10", "a")
	ZAdd(ctx)
	ctx.Args = makeArgs("undo_zset", "20", "a", "30", "b")
	undoCmds := UndoZAdd(ctx)
	if len(undoCmds) != 2 {
		t.Fatalf("Expected 2 undo cmds, got %d", len(undoCmds))
	}
	hasRestoreA := false
	hasRemB := false
	for _, cmd := range undoCmds {
		if string(cmd[0]) == "ZADD" && string(cmd[3]) == "a" && string(cmd[2]) == "10" {
			hasRestoreA = true
		}
		if string(cmd[0]) == "ZREM" && string(cmd[2]) == "b" {
			hasRemB = true
		}
	}
	if !hasRestoreA || !hasRemB {
		t.Errorf("UndoZAdd logic incorrect: %v", undoCmds)
	}
}