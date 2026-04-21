package engine

import "testing"

func TestSystem(t *testing.T) {
	t.Parallel()

	eng := New(1)
	db := eng.DB(0)

	if got := db.Type("missing"); got != "none" {
		t.Fatalf("unexpected missing type: %q", got)
	}

	db.Set("a", []byte("1"))
	if got := db.Type("a"); got != "string" {
		t.Fatalf("unexpected string type: %q", got)
	}

	if got := db.DBSize(); got != 1 {
		t.Fatalf("unexpected dbsize after set: %d", got)
	}

	if _, err := db.LPush("list", []byte("x")); err != nil {
		t.Fatalf("unexpected lpush error: %v", err)
	}
	if got := db.Type("list"); got != "list" {
		t.Fatalf("unexpected list type: %q", got)
	}

	if got := db.DBSize(); got != 2 {
		t.Fatalf("unexpected dbsize after list: %d", got)
	}
}