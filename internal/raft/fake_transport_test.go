package raft_test

import (
	"context"
	"testing"
	"time"

	raft "mini-kv/internal/raft"
	"mini-kv/internal/raft/logstore"
)

func TestFake(t *testing.T) {
	transport := raft.NewFakeTransport()
	storage := logstore.NewMemoryStorage()

	node, err := raft.NewNode(raft.Config{
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

	handler, ok := node.(raft.RPCHandler)
	if !ok {
		t.Fatalf("node should implement RPCHandler")
	}
	transport.Register("node1", handler)

	resp, err := transport.RequestVote(context.Background(), "node1", raft.RequestVoteRequest{
		Term:         1,
		CandidateID:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("request vote error: %v", err)
	}
	if resp.Term != 1 {
		t.Fatalf("term = %d, want 1", resp.Term)
	}
	if !resp.VoteGranted {
		t.Fatalf("vote should be granted")
	}
}
