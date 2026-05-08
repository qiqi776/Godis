package raft_test

import (
	"context"
	"testing"
	"time"

	raft "mini-kv/internal/raft"
	"mini-kv/internal/raft/logstore"
)

func TestNewNode(t *testing.T) {
	storage := logstore.NewMemoryStorage()
	transport := raft.NewFakeTransport()

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

	if node.IsLeader() {
		t.Fatalf("new node should start as follower")
	}
	if node.LeaderID() != "" {
		t.Fatalf("new node should not know leader")
	}
}

func TestMemoryStorageAppend(t *testing.T) {
	storage := logstore.NewMemoryStorage()

	err := storage.Append([]raft.LogEntry{
		{Index: 1, Term: 1, Type: raft.EntryNormal, Data: []byte("a")},
		{Index: 2, Term: 1, Type: raft.EntryNormal, Data: []byte("b")},
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

func TestSingleLeaderElection(t *testing.T) {
	transport := raft.NewFakeTransport()
	peers := []string{"node1", "node2", "node3"}

	nodes := make([]raft.Node, 0, len(peers))
	for _, id := range peers {
		node, err := raft.NewNode(raft.Config{
			ID:               id,
			Peers:            peers,
			Storage:          logstore.NewMemoryStorage(),
			Transport:        transport,
			ElectionTimeout:  80 * time.Millisecond,
			HeartbeatTimeout: 20 * time.Millisecond,
			ApplyBufferSize:  16,
		})
		if err != nil {
			t.Fatalf("new node %s error: %v", id, err)
		}

		handler, ok := node.(raft.RPCHandler)
		if !ok {
			t.Fatalf("node %s should implement RPCHandler", id)
		}
		transport.Register(id, handler)
		nodes = append(nodes, node)
	}

	for _, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("start node error: %v", err)
		}
	}
	defer func() {
		for _, node := range nodes {
			_ = node.Stop()
		}
	}()

	leader := waitLeader(t, nodes, time.Second)
	if leader == "" {
		t.Fatalf("leader should be elected")
	}

	leaderCount := 0
	for _, node := range nodes {
		if node.IsLeader() {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("leader count = %d, want 1", leaderCount)
	}
}

func TestLeaderFailover(t *testing.T) {
	transport := raft.NewFakeTransport()
	peers := []string{"node1", "node2", "node3"}

	nodes := make(map[string]raft.Node)
	for _, id := range peers {
		node, err := raft.NewNode(raft.Config{
			ID:               id,
			Peers:            peers,
			Storage:          logstore.NewMemoryStorage(),
			Transport:        transport,
			ElectionTimeout:  80 * time.Millisecond,
			HeartbeatTimeout: 20 * time.Millisecond,
			ApplyBufferSize:  16,
		})
		if err != nil {
			t.Fatalf("new node %s error: %v", id, err)
		}

		handler, ok := node.(raft.RPCHandler)
		if !ok {
			t.Fatalf("node %s should implement RPCHandler", id)
		}
		transport.Register(id, handler)
		nodes[id] = node
	}

	for _, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("start node error: %v", err)
		}
	}
	defer func() {
		for _, node := range nodes {
			_ = node.Stop()
		}
	}()

	firstLeader := waitLeaderFromMap(t, nodes, time.Second)
	if firstLeader == "" {
		t.Fatalf("first leader should be elected")
	}

	_ = nodes[firstLeader].Stop()
	transport.Unregister(firstLeader)

	delete(nodes, firstLeader)
	secondLeader := waitLeaderFromMap(t, nodes, time.Second)
	if secondLeader == "" {
		t.Fatalf("new leader should be elected")
	}
	if secondLeader == firstLeader {
		t.Fatalf("new leader should differ from stopped leader")
	}
}

func waitLeader(t *testing.T, nodes []raft.Node, timeout time.Duration) string {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leader string
		leaderCount := 0
		for _, node := range nodes {
			if node.IsLeader() {
				leader = node.LeaderID()
				leaderCount++
			}
		}
		if leaderCount == 1 {
			return leader
		}
		time.Sleep(10 * time.Millisecond)
	}
	return ""
}

func waitLeaderFromMap(t *testing.T, nodes map[string]raft.Node, timeout time.Duration) string {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id, node := range nodes {
			if node.IsLeader() {
				return id
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return ""
}

func TestLeaderPropose(t *testing.T) {
	transport := raft.NewFakeTransport()
	peers := []string{"node1", "node2", "node3"}

	nodes := make(map[string]raft.Node)
	for _, id := range peers {
		node, err := raft.NewNode(raft.Config{
			ID:               id,
			Peers:            peers,
			Storage:          logstore.NewMemoryStorage(),
			Transport:        transport,
			ElectionTimeout:  80 * time.Millisecond,
			HeartbeatTimeout: 20 * time.Millisecond,
			ApplyBufferSize:  16,
		})
		if err != nil {
			t.Fatalf("new node %s error: %v", id, err)
		}

		handler, ok := node.(raft.RPCHandler)
		if !ok {
			t.Fatalf("node %s should implement RPCHandler", id)
		}
		transport.Register(id, handler)
		nodes[id] = node
	}

	for _, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("start node error: %v", err)
		}
	}
	defer func() {
		for _, node := range nodes {
			_ = node.Stop()
		}
	}()

	leaderID := waitLeaderFromMap(t, nodes, time.Second)
	if leaderID == "" {
		t.Fatalf("leader should be elected")
	}

	leader := nodes[leaderID]
	index, err := leader.Propose(context.Background(), []byte("cmd-1"))
	if err != nil {
		t.Fatalf("propose error: %v", err)
	}
	if index == 0 {
		t.Fatalf("propose should return non-zero index")
	}

	msg := waitApply(t, leader.ApplyCh(), time.Second)
	if msg.Index != index {
		t.Fatalf("apply index = %d, want %d", msg.Index, index)
	}
	if string(msg.Data) != "cmd-1" {
		t.Fatalf("apply data = %q, want cmd-1", msg.Data)
	}
}

func TestFollowerRejectsPropose(t *testing.T) {
	transport := raft.NewFakeTransport()
	peers := []string{"node1", "node2", "node3"}

	nodes := make(map[string]raft.Node)
	for _, id := range peers {
		node, err := raft.NewNode(raft.Config{
			ID:               id,
			Peers:            peers,
			Storage:          logstore.NewMemoryStorage(),
			Transport:        transport,
			ElectionTimeout:  80 * time.Millisecond,
			HeartbeatTimeout: 20 * time.Millisecond,
			ApplyBufferSize:  16,
		})
		if err != nil {
			t.Fatalf("new node %s error: %v", id, err)
		}

		handler, ok := node.(raft.RPCHandler)
		if !ok {
			t.Fatalf("node %s should implement RPCHandler", id)
		}
		transport.Register(id, handler)
		nodes[id] = node
	}

	for _, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("start node error: %v", err)
		}
	}
	defer func() {
		for _, node := range nodes {
			_ = node.Stop()
		}
	}()

	leaderID := waitLeaderFromMap(t, nodes, time.Second)
	if leaderID == "" {
		t.Fatalf("leader should be elected")
	}

	for id, node := range nodes {
		if id == leaderID {
			continue
		}

		_, err := node.Propose(context.Background(), []byte("cmd-1"))
		if err != raft.ErrNotLeader {
			t.Fatalf("follower propose error = %v, want %v", err, raft.ErrNotLeader)
		}
		return
	}

	t.Fatalf("no follower found")
}

func waitApply(t *testing.T, ch <-chan raft.ApplyMsg, timeout time.Duration) raft.ApplyMsg {
	t.Helper()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				t.Fatalf("apply channel closed")
			}
			if msg.Type == raft.EntryNoop {
				continue
			}
			return msg
		case <-timer.C:
			t.Fatalf("timed out waiting for apply")
			return raft.ApplyMsg{}
		}
	}
}
