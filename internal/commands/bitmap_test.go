package commands

import (
	"godis/internal/core"
	"godis/internal/database"
	"testing"
)

func TestBitmapCommands(t *testing.T) {
	db := database.NewStandalone()
	ctx := &core.Context{
		DB:   db,
		Conn: core.NewConnection(),
	}
	ctx.Args = makeArgs("mybit", "7", "1")
	res := SetBit(ctx)
	if res.Num != 0 {
		t.Errorf("SETBIT first time should return 0, got %d", res.Num)
	}
	ctx.Args = makeArgs("mybit", "7")
	res = GetBit(ctx)
	if res.Num != 1 {
		t.Errorf("GETBIT 7 expected 1, got %d", res.Num)
	}
	ctx.Args = makeArgs("mybit", "0")
	res = GetBit(ctx)
	if res.Num != 0 {
		t.Errorf("GETBIT 0 expected 0, got %d", res.Num)
	}
	ctx.Args = makeArgs("mybit", "7", "0")
	res = SetBit(ctx)
	if res.Num != 1 {
		t.Errorf("SETBIT reset expected old val 1, got %d", res.Num)
	}
	ctx.Args = makeArgs("count_key", "0", "1")
	SetBit(ctx)
	ctx.Args = makeArgs("count_key", "8", "1")
	SetBit(ctx)
	ctx.Args = makeArgs("count_key")
	res = BitCount(ctx)
	if res.Num != 2 {
		t.Errorf("BITCOUNT expected 2, got %d", res.Num)
	}
	ctx.Args = makeArgs("count_key", "0", "0")
	res = BitCount(ctx)
	if res.Num != 1 {
		t.Errorf("BITCOUNT range 0 0 expected 1, got %d", res.Num)
	}
}

func TestUndoSetBit(t *testing.T) {
	db := database.NewStandalone()
	ctx := &core.Context{
		DB:   db,
		Conn: core.NewConnection(),
	}
	ctx.Args = makeArgs("undo_bit", "7", "1")
	SetBit(ctx)
	ctx.Args = makeArgs("undo_bit", "7", "0")
	undoCmds := UndoSetBit(ctx)
	if len(undoCmds) != 1 {
		t.Fatalf("Expected 1 undo cmd, got %d", len(undoCmds))
	}
	cmd := undoCmds[0]
	if string(cmd[0]) != "SETBIT" || string(cmd[2]) != "7" || string(cmd[3]) != "1" {
		t.Errorf("Undo command mismatch: %v", cmd)
	}
}