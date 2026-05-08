package logstore

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"mini-kv/internal/raft"
)

func TestFileStoragePersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")

	storage, err := OpenFileStorage(path)
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}

	if err := storage.SaveHardState(raft.HardState{
		CurrentTerm: 2,
		VotedFor:    "node1",
		Commit:      0,
	}); err != nil {
		t.Fatalf("save hard state: %v", err)
	}

	if err := storage.Append([]raft.LogEntry{
		{Index: 1, Term: 2, Type: raft.EntryNormal, Data: []byte("a")},
		{Index: 2, Term: 2, Type: raft.EntryNormal, Data: []byte("b")},
	}); err != nil {
		t.Fatalf("append entries: %v", err)
	}

	if err := storage.SaveHardState(raft.HardState{
		CurrentTerm: 2,
		VotedFor:    "node1",
		Commit:      2,
	}); err != nil {
		t.Fatalf("save committed hard state: %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("close storage: %v", err)
	}

	recovered, err := OpenFileStorage(path)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	defer recovered.Close()

	state, err := recovered.LoadHardState()
	if err != nil {
		t.Fatalf("load hard state: %v", err)
	}
	if state.CurrentTerm != 2 || state.VotedFor != "node1" || state.Commit != 2 {
		t.Fatalf("hard state = %+v, want term=2 vote=node1 commit=2", state)
	}

	lastIndex, err := recovered.LastIndex()
	if err != nil {
		t.Fatalf("last index: %v", err)
	}
	if lastIndex != 2 {
		t.Fatalf("last index = %d, want 2", lastIndex)
	}

	entries, err := recovered.Entries(1, 3)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	if len(entries) != 2 || string(entries[0].Data) != "a" || string(entries[1].Data) != "b" {
		t.Fatalf("unexpected entries: %+v", entries)
	}
}

func TestFileStorageTruncate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")

	storage, err := OpenFileStorage(path)
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}

	if err := storage.Append([]raft.LogEntry{
		{Index: 1, Term: 1, Type: raft.EntryNormal, Data: []byte("a")},
		{Index: 2, Term: 1, Type: raft.EntryNormal, Data: []byte("b")},
		{Index: 3, Term: 1, Type: raft.EntryNormal, Data: []byte("c")},
	}); err != nil {
		t.Fatalf("append entries: %v", err)
	}

	if err := storage.TruncateSuffix(1); err != nil {
		t.Fatalf("truncate suffix: %v", err)
	}

	if err := storage.Append([]raft.LogEntry{
		{Index: 2, Term: 2, Type: raft.EntryNormal, Data: []byte("x")},
	}); err != nil {
		t.Fatalf("append replacement: %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("close storage: %v", err)
	}

	recovered, err := OpenFileStorage(path)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	defer recovered.Close()

	lastIndex, err := recovered.LastIndex()
	if err != nil {
		t.Fatalf("last index: %v", err)
	}
	if lastIndex != 2 {
		t.Fatalf("last index = %d, want 2", lastIndex)
	}

	term, err := recovered.Term(2)
	if err != nil {
		t.Fatalf("term: %v", err)
	}
	if term != 2 {
		t.Fatalf("term = %d, want 2", term)
	}

	entries, err := recovered.Entries(2, 3)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	if len(entries) != 1 || string(entries[0].Data) != "x" {
		t.Fatalf("unexpected entries: %+v", entries)
	}
}

func TestFileStorageSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")

	storage, err := OpenFileStorage(path)
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}

	if err := storage.Append([]raft.LogEntry{
		{Index: 1, Term: 1, Type: raft.EntryNormal, Data: []byte("a")},
		{Index: 2, Term: 1, Type: raft.EntryNormal, Data: []byte("b")},
		{Index: 3, Term: 2, Type: raft.EntryNormal, Data: []byte("c")},
	}); err != nil {
		t.Fatalf("append entries: %v", err)
	}

	if err := storage.SaveSnapshot(raft.Snapshot{
		Index: 2,
		Term:  1,
		Data:  []byte("snapshot-data"),
	}); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	if _, err := storage.Entries(1, 2); err != raft.ErrCompacted {
		t.Fatalf("entries before snapshot error = %v, want %v", err, raft.ErrCompacted)
	}

	term, err := storage.Term(2)
	if err != nil {
		t.Fatalf("snapshot term: %v", err)
	}
	if term != 1 {
		t.Fatalf("snapshot term = %d, want 1", term)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("close storage: %v", err)
	}

	recovered, err := OpenFileStorage(path)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	defer recovered.Close()

	snapshot, err := recovered.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if snapshot.Index != 2 || snapshot.Term != 1 || string(snapshot.Data) != "snapshot-data" {
		t.Fatalf("snapshot = %+v, want index=2 term=1 data=snapshot-data", snapshot)
	}

	lastIndex, err := recovered.LastIndex()
	if err != nil {
		t.Fatalf("last index: %v", err)
	}
	if lastIndex != 3 {
		t.Fatalf("last index = %d, want 3", lastIndex)
	}

	entries, err := recovered.Entries(2, 4)
	if err != nil {
		t.Fatalf("entries after snapshot: %v", err)
	}
	if len(entries) != 2 || entries[0].Index != 2 || entries[1].Index != 3 || string(entries[1].Data) != "c" {
		t.Fatalf("unexpected entries after snapshot: %+v", entries)
	}
}

func TestFileStorageApplySnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")

	storage, err := OpenFileStorage(path)
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}

	if err := storage.ApplySnapshot(raft.Snapshot{
		Index: 5,
		Term:  3,
		Data:  []byte("remote-snapshot"),
	}); err != nil {
		t.Fatalf("apply snapshot: %v", err)
	}

	if _, err := storage.Term(4); err != raft.ErrCompacted {
		t.Fatalf("term before snapshot error = %v, want %v", err, raft.ErrCompacted)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("close storage: %v", err)
	}

	recovered, err := OpenFileStorage(path)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	defer recovered.Close()

	snapshot, err := recovered.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if snapshot.Index != 5 || snapshot.Term != 3 || string(snapshot.Data) != "remote-snapshot" {
		t.Fatalf("snapshot = %+v, want index=5 term=3 data=remote-snapshot", snapshot)
	}

	lastIndex, err := recovered.LastIndex()
	if err != nil {
		t.Fatalf("last index: %v", err)
	}
	if lastIndex != 5 {
		t.Fatalf("last index = %d, want 5", lastIndex)
	}
}

func TestNodeRestartApply(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")

	storage, err := OpenFileStorage(path)
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}

	if err := storage.Append([]raft.LogEntry{
		{Index: 1, Term: 1, Type: raft.EntryNormal, Data: []byte("restore-me")},
	}); err != nil {
		t.Fatalf("append entry: %v", err)
	}
	if err := storage.SaveHardState(raft.HardState{
		CurrentTerm: 1,
		VotedFor:    "node1",
		Commit:      1,
	}); err != nil {
		t.Fatalf("save hard state: %v", err)
	}
	if err := storage.Close(); err != nil {
		t.Fatalf("close storage: %v", err)
	}

	recovered, err := OpenFileStorage(path)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	defer recovered.Close()

	node, err := raft.NewNode(raft.Config{
		ID:               "node1",
		Peers:            []string{"node1"},
		Storage:          recovered,
		Transport:        raft.NewFakeTransport(),
		ElectionTimeout:  time.Second,
		HeartbeatTimeout: 100 * time.Millisecond,
		ApplyBufferSize:  16,
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	defer node.Stop()

	if err := node.Start(); err != nil {
		t.Fatalf("start node: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	select {
	case msg := <-node.ApplyCh():
		if msg.Index != 1 || string(msg.Data) != "restore-me" {
			t.Fatalf("apply msg = %+v, want index=1 data=restore-me", msg)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for restored committed entry")
	}
}

func TestNodeRestartVote(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")

	storage, err := OpenFileStorage(path)
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}

	node, err := raft.NewNode(raft.Config{
		ID:               "node1",
		Peers:            []string{"node1", "node2", "node3"},
		Storage:          storage,
		Transport:        raft.NewFakeTransport(),
		ElectionTimeout:  time.Second,
		HeartbeatTimeout: 100 * time.Millisecond,
		ApplyBufferSize:  16,
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	handler, ok := node.(raft.RPCHandler)
	if !ok {
		t.Fatalf("node should implement RPCHandler")
	}

	resp, err := handler.HandleRequestVote(context.Background(), raft.RequestVoteRequest{
		Term:         1,
		CandidateID:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("request vote: %v", err)
	}
	if !resp.VoteGranted {
		t.Fatalf("first vote should be granted")
	}

	_ = node.Stop()
	if err := storage.Close(); err != nil {
		t.Fatalf("close storage: %v", err)
	}

	recovered, err := OpenFileStorage(path)
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	defer recovered.Close()

	restarted, err := raft.NewNode(raft.Config{
		ID:               "node1",
		Peers:            []string{"node1", "node2", "node3"},
		Storage:          recovered,
		Transport:        raft.NewFakeTransport(),
		ElectionTimeout:  time.Second,
		HeartbeatTimeout: 100 * time.Millisecond,
		ApplyBufferSize:  16,
	})
	if err != nil {
		t.Fatalf("new restarted node: %v", err)
	}
	defer restarted.Stop()

	restartedHandler, ok := restarted.(raft.RPCHandler)
	if !ok {
		t.Fatalf("restarted node should implement RPCHandler")
	}

	resp, err = restartedHandler.HandleRequestVote(context.Background(), raft.RequestVoteRequest{
		Term:         1,
		CandidateID:  "node3",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("second request vote: %v", err)
	}
	if resp.VoteGranted {
		t.Fatalf("second vote in same term should be rejected after restart")
	}
	if resp.Term != 1 {
		t.Fatalf("term = %d, want 1", resp.Term)
	}
}
