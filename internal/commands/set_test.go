package commands

import (
	"godis/internal/core"
	"godis/internal/database"
	"testing"
)

func TestSetCommands(t *testing.T) {
	db := database.NewStandalone()
	ctx := &core.Context{
		DB:   db,
		Conn: core.NewConnection(),
	}
	ctx.Args = makeArgs("myset", "a", "b", "c")
	res := SAdd(ctx)
	if res.Num != 3 {
		t.Errorf("SADD expected 3, got %d", res.Num)
	}
	ctx.Args = makeArgs("myset", "c", "d")
	res = SAdd(ctx)
	if res.Num != 1 {
		t.Errorf("SADD duplicate expected 1, got %d", res.Num)
	}
	ctx.Args = makeArgs("myset", "a")
	res = SIsMember(ctx)
	if res.Num != 1 {
		t.Error("SIsMember 'a' expected 1")
	}
	ctx.Args = makeArgs("myset", "z")
	res = SIsMember(ctx)
	if res.Num != 0 {
		t.Error("SIsMember 'z' expected 0")
	}
	ctx.Args = makeArgs("myset")
	res = SCard(ctx)
	if res.Num != 4 {
		t.Errorf("SCARD expected 4, got %d", res.Num)
	}
	ctx.Args = makeArgs("myset")
	res = SMembers(ctx)
	if len(res.Array) != 4 {
		t.Errorf("SMEMBERS expected len 4, got %d", len(res.Array))
	}
	ctx.Args = makeArgs("myset", "b", "d", "x")
	res = SRem(ctx)
	if res.Num != 2 {
		t.Errorf("SREM expected 2, got %d", res.Num)
	}
	ctx.Args = makeArgs("myset")
	res = SCard(ctx)
	if res.Num != 2 {
		t.Errorf("After SREM, SCARD expected 2, got %d", res.Num)
	}
}

func TestSAddUndo(t *testing.T) {
	db := database.NewStandalone()
	ctx := &core.Context{
		DB:   db,
		Conn: core.NewConnection(),
	}
	ctx.Args = makeArgs("undo_set", "a", "b")
	SAdd(ctx)
	ctx.Args = makeArgs("undo_set", "b", "c", "d")
	undoCmds := UndoSAdd(ctx)
	if len(undoCmds) != 2 {
		t.Errorf("Expected 2 undo commands, got %d", len(undoCmds))
	}
	cmd1 := undoCmds[0]
	if string(cmd1[0]) != "SREM" {
		t.Errorf("Undo command expected SREM, got %s", string(cmd1[0]))
	}
}