package engine

import (
	"bytes"
	"testing"
	"time"
)

func TestFSMApplyPutAndDelete(t *testing.T) {
	engine := New(1)

	result := engine.Apply(KVCommand{
		Type:  CommandPut,
		Key:   "k1",
		Value: []byte("v1"),
	})
	if result.Error != "" {
		t.Fatalf("apply put error: %s", result.Error)
	}
	if !result.Found {
		t.Fatalf("apply put should report found")
	}

	value, ok, err := engine.DB(0).Get("k1")
	if err != nil {
		t.Fatalf("get error: %v", err)
	}
	if !ok || !bytes.Equal(value, []byte("v1")) {
		t.Fatalf("unexpected value: ok=%v value=%q", ok, value)
	}

	result = engine.Apply(KVCommand{
		Type: CommandDelete,
		Key:  "k1",
	})
	if result.Error != "" {
		t.Fatalf("apply delete error: %s", result.Error)
	}
	if !result.Found {
		t.Fatalf("apply delete should report found")
	}

	_, ok, err = engine.DB(0).Get("k1")
	if err != nil {
		t.Fatalf("get after delete error: %v", err)
	}
	if ok {
		t.Fatalf("key should be deleted")
	}
}

func TestFSMApplyExpireAndPersist(t *testing.T) {
	engine := New(1)

	result := engine.Apply(KVCommand{
		Type:  CommandPut,
		Key:   "k1",
		Value: []byte("v1"),
	})
	if result.Error != "" {
		t.Fatalf("apply put error: %s", result.Error)
	}

	result = engine.Apply(KVCommand{
		Type:     CommandExpire,
		Key:      "k1",
		ExpireAt: time.Now().Add(time.Hour).UnixMilli(),
	})
	if result.Error != "" {
		t.Fatalf("apply expire error: %s", result.Error)
	}
	if !result.Found {
		t.Fatalf("apply expire should report found")
	}
	if ttl := engine.DB(0).TTL("k1"); ttl <= 0 {
		t.Fatalf("ttl should be positive, got %d", ttl)
	}

	result = engine.Apply(KVCommand{
		Type: CommandPersist,
		Key:  "k1",
	})
	if result.Error != "" {
		t.Fatalf("apply persist error: %s", result.Error)
	}
	if !result.Found {
		t.Fatalf("apply persist should report found")
	}
	if ttl := engine.DB(0).TTL("k1"); ttl != -1 {
		t.Fatalf("ttl should be removed, got %d", ttl)
	}
}

func TestFSMSnapshotRestore(t *testing.T) {
	engine := New(1)
	expireAt := time.Now().Add(time.Hour).UnixMilli()

	for _, command := range []KVCommand{
		{Type: CommandPut, Key: "k1", Value: []byte("v1")},
		{Type: CommandPut, Key: "k2", Value: []byte("v2"), ExpireAt: expireAt},
	} {
		result := engine.Apply(command)
		if result.Error != "" {
			t.Fatalf("apply error: %s", result.Error)
		}
	}

	data, err := engine.Snapshot()
	if err != nil {
		t.Fatalf("snapshot error: %v", err)
	}

	restored := New(1)
	if err := restored.Restore(data); err != nil {
		t.Fatalf("restore error: %v", err)
	}

	value, ok, err := restored.DB(0).Get("k1")
	if err != nil {
		t.Fatalf("get k1 error: %v", err)
	}
	if !ok || !bytes.Equal(value, []byte("v1")) {
		t.Fatalf("unexpected k1: ok=%v value=%q", ok, value)
	}

	value, ok, err = restored.DB(0).Get("k2")
	if err != nil {
		t.Fatalf("get k2 error: %v", err)
	}
	if !ok || !bytes.Equal(value, []byte("v2")) {
		t.Fatalf("unexpected k2: ok=%v value=%q", ok, value)
	}
	if ttl := restored.DB(0).TTL("k2"); ttl <= 0 {
		t.Fatalf("restored ttl should be positive, got %d", ttl)
	}
}

func TestFSMApplyUnknownCommand(t *testing.T) {
	engine := New(1)

	result := engine.Apply(KVCommand{
		Type: CommandType(255),
		Key:  "k1",
	})
	if result.Error == "" {
		t.Fatalf("unknown command should return error")
	}
}

func TestFSMRestoreInvalidSnapshot(t *testing.T) {
	engine := New(1)

	if err := engine.Restore([]byte("not-json")); err == nil {
		t.Fatalf("restore invalid snapshot should fail")
	}
}
