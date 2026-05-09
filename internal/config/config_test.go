package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mini-kv.yaml")
	data := []byte("port: 0\nraft:\n  id: node2\n")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.Port != 0 {
		t.Fatalf("port = %d, want 0", cfg.Port)
	}
	if cfg.Raft.ID != "node2" {
		t.Fatalf("raft id = %q, want node2", cfg.Raft.ID)
	}
	if len(cfg.Raft.Peers) != 1 || cfg.Raft.Peers[0] != "node2" {
		t.Fatalf("raft peers = %v, want [node2]", cfg.Raft.Peers)
	}
	if cfg.Raft.WALPath != "data/raft-node2.wal" {
		t.Fatalf("raft wal path = %q, want data/raft-node2.wal", cfg.Raft.WALPath)
	}
	if cfg.Raft.ElectionTimeoutMS != Default().Raft.ElectionTimeoutMS {
		t.Fatalf("election timeout = %d, want %d", cfg.Raft.ElectionTimeoutMS, Default().Raft.ElectionTimeoutMS)
	}
	if cfg.Raft.HeartbeatTimeoutMS != Default().Raft.HeartbeatTimeoutMS {
		t.Fatalf("heartbeat timeout = %d, want %d", cfg.Raft.HeartbeatTimeoutMS, Default().Raft.HeartbeatTimeoutMS)
	}
	if cfg.Raft.ApplyBufferSize != Default().Raft.ApplyBufferSize {
		t.Fatalf("apply buffer size = %d, want %d", cfg.Raft.ApplyBufferSize, Default().Raft.ApplyBufferSize)
	}
}
