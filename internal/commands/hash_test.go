package commands

import (
	"godis/internal/core"
	"godis/internal/database"
	"testing"
)

func TestHashCommands(t *testing.T) {
	db := database.NewStandalone()
	ctx := &core.Context{
		DB:   db,
		Conn: core.NewConnection(),
	}
	ctx.Args = makeArgs("myhash", "f1", "v1", "f2", "v2")
	res := HSet(ctx)
	if res.Num != 2 {
		t.Errorf("HSET expected 2 added, got %d", res.Num)
	}
	ctx.Args = makeArgs("myhash", "f1")
	res = HGet(ctx)
	if string(res.Bulk) != "v1" {
		t.Errorf("HGET f1 expected 'v1', got %s", res.Bulk)
	}
	ctx.Args = makeArgs("myhash", "f1")
	res = HExists(ctx)
	if res.Num != 1 {
		t.Error("HEXISTS f1 expected 1")
	}
	ctx.Args = makeArgs("myhash", "fx")
	res = HExists(ctx)
	if res.Num != 0 {
		t.Error("HEXISTS fx expected 0")
	}
	ctx.Args = makeArgs("myhash")
	res = HLen(ctx)
	if res.Num != 2 {
		t.Errorf("HLEN expected 2, got %d", res.Num)
	}
	ctx.Args = makeArgs("myhash")
	res = HGetAll(ctx)
	if len(res.Array) != 4 {
		t.Errorf("HGETALL expected len 4, got %d", len(res.Array))
	}
	ctx.Args = makeArgs("myhash", "f1", "f3")
	res = HDel(ctx)
	if res.Num != 1 {
		t.Errorf("HDEL expected 1, got %d", res.Num)
	}
}

func TestHSetUndo(t *testing.T) {
	db := database.NewStandalone()
	ctx := &core.Context{
		DB:   db,
		Conn: core.NewConnection(),
	}
	ctx.Args = makeArgs("undo_hash", "f1", "v1")
	HSet(ctx)
	ctx.Args = makeArgs("undo_hash", "f1", "new_v1", "f2", "v2")
	undoCmds := UndoHSet(ctx)
	if len(undoCmds) != 2 {
		t.Errorf("Expected 2 undo commands, got %d", len(undoCmds))
	}
	hasSetRestore := false
	hasDel := false
	for _, cmd := range undoCmds {
		cmdName := string(cmd[0])
		if cmdName == "HSET" && string(cmd[2]) == "f1" && string(cmd[3]) == "v1" {
			hasSetRestore = true
		}
		if cmdName == "HDEL" && string(cmd[2]) == "f2" {
			hasDel = true
		}
	}
	if !hasSetRestore || !hasDel {
		t.Errorf("Undo commands logic error: %v", undoCmds)
	}
}

func TestHDelUndo(t *testing.T) {
	db := database.NewStandalone()
	ctx := &core.Context{
		DB:   db,
		Conn: core.NewConnection(),
	}
	ctx.Args = makeArgs("del_hash", "f1", "v1", "f2", "v2")
	HSet(ctx)
	ctx.Args = makeArgs("del_hash", "f1", "f3")
	undoCmds := UndoHDel(ctx)
	if len(undoCmds) != 1 {
		t.Errorf("Expected 1 undo command, got %d", len(undoCmds))
	}
	cmd := undoCmds[0]
	if string(cmd[0]) != "HSET" || string(cmd[2]) != "f1" || string(cmd[3]) != "v1" {
		t.Errorf("Undo command mismatch: %v", cmd)
	}
}
