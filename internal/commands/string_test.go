package commands

import (
	"godis/internal/core"
	"godis/internal/database"
	"testing"
)

func TestStringCommands(t *testing.T) {
	db := database.NewStandalone()
	ctx := &core.Context{
		DB:   db,
		Conn: core.NewConnection(),
	}
	ctx.Args = makeArgs("k1", "v1")
	res := Set(ctx)
	if res.Str != "OK" {
		t.Error("SET failed")
	}
	ctx.Args = makeArgs("k1")
	res = Get(ctx)
	if string(res.Bulk) != "v1" {
		t.Error("GET mismatch")
	}
	ctx.Args = makeArgs("k1", "v2")
	res = SetNX(ctx)
	if res.Num != 0 {
		t.Error("SETNX existing key should return 0")
	}
	ctx.Args = makeArgs("k2", "v2")
	res = SetNX(ctx)
	if res.Num != 1 {
		t.Error("SETNX new key should return 1")
	}
	ctx.Args = makeArgs("k1")
	res = StrLen(ctx)
	if res.Num != 2 {
		t.Error("STRLEN expected 2")
	}
	ctx.Args = makeArgs("mk1", "mv1", "mk2", "mv2")
	MSet(ctx)
	ctx.Args = makeArgs("mk1", "mk2", "none")
	res = MGet(ctx)
	if len(res.Array) != 3 {
		t.Error("MGET expected 3 results")
	}
	if string(res.Array[0].Bulk) != "mv1" || string(res.Array[1].Bulk) != "mv2" || res.Array[2].Bulk != nil {
		t.Error("MGET values mismatch")
	}
}

func TestUndoSet(t *testing.T) {
	db := database.NewStandalone()
	ctx := &core.Context{
		DB:   db,
		Conn: core.NewConnection(),
	}
	ctx.Args = makeArgs("undo_new", "val")
	undo := UndoSet(ctx)
	if len(undo) != 1 || string(undo[0][0]) != "DEL" {
		t.Error("UndoSet (new) should return DEL")
	}
	ctx.Args = makeArgs("undo_old", "old_val")
	Set(ctx)
	ctx.Args = makeArgs("undo_old", "new_val")
	undo = UndoSet(ctx)
	if len(undo) != 1 || string(undo[0][0]) != "SET" || string(undo[0][2]) != "old_val" {
		t.Error("UndoSet (update) failed")
	}
}