package manifest

import (
	"testing"

	version "mini-kv/internal/storage/lsm/sstable"
)

func TestStoreApplyAndLoad(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, 1)
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	edit := version.Edit{
		NextFileNum: 3,
		LastSeq:     9,
		Added: []version.TableMeta{{
			FileNum:  2,
			Level:    0,
			Smallest: []byte("a"),
			Largest:  []byte("z"),
			MinSeq:   1,
			MaxSeq:   9,
			Size:     100,
		}},
	}
	if err := store.Apply(edit); err != nil {
		t.Fatalf("Apply error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	store, err = Open(dir, 1)
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer func() { _ = store.Close() }()
	state, err := store.Load()
	if err != nil {
		t.Fatalf("Load error = %v", err)
	}
	if state.LastSeq != 9 || state.NextFileNum != 3 || len(state.Levels) == 0 || len(state.Levels[0]) != 1 {
		t.Fatalf("state = %+v, want replayed edit", state)
	}
}
