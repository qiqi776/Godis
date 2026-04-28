package raft

import (
	"context"
	"testing"
	"time"
)

func TestNewNode(t *testing.T) {
	storage := NewMemoryStorage()
	transport := NewFakeTransport()

	node, err := NewNode(Config{
		ID:               "node1",
		Peers:            []string{"node1", "node2", "node3"},
		Storage:          storage,
		Transport:        transport,
		ElectionTimeout:  time.Second,
		HeartbeatTimeout: 100 * time.Millisecond,
		ApplyBufferSize:  16,
	})
	if err != nil {
		t.Fatalf("new node error: %v", err)
	}

	if node.IsLeader() {
		t.Fatalf("new node should start as follower")
	}
	if node.LeaderID() != "" {
		t.Fatalf("new node should not know leader")
	}
}

func TestMemoryStorageAppendAndRead(t *testing.T) {
	storage := NewMemoryStorage()

	err := storage.Append([]LogEntry{
		{Index: 1, Term: 1, Type: EntryNormal, Data: []byte("a")},
		{Index: 2, Term: 1, Type: EntryNormal, Data: []byte("b")},
	})
	if err != nil {
		t.Fatalf("append error: %v", err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		t.Fatalf("last index error: %v", err)
	}
	if lastIndex != 2 {
		t.Fatalf("last index = %d, want 2", lastIndex)
	}

	term, err := storage.Term(2)
	if err != nil {
		t.Fatalf("term error: %v", err)
	}
	if term != 1 {
		t.Fatalf("term = %d, want 1", term)
	}

	entries, err := storage.Entries(1, 3)
	if err != nil {
		t.Fatalf("entries error: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("len(entries) = %d, want 2", len(entries))
	}
}

func TestFakeTransport(t *testing.T) {
	transport := NewFakeTransport()
	storage := NewMemoryStorage()

	node, err := NewNode(Config{
		ID:               "node1",
		Peers:            []string{"node1", "node2", "node3"},
		Storage:          storage,
		Transport:        transport,
		ElectionTimeout:  time.Second,
		HeartbeatTimeout: 100 * time.Millisecond,
		ApplyBufferSize:  16,
	})
	if err != nil {
		t.Fatalf("new node error: %v", err)
	}

	handler, ok := node.(RPCHandler)
	if !ok {
		t.Fatalf("node should implement RPCHandler")
	}
	transport.Register("node1", handler)

	resp, err := transport.RequestVote(context.Background(), "node1", RequestVoteRequest{
		Term:         1,
		CandidateID:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("request vote error: %v", err)
	}
	if resp.Term != 0 {
		t.Fatalf("term = %d, want 0", resp.Term)
	}
}
