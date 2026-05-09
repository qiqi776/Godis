package raft_test

import (
	"context"
	"testing"
	"time"

	raft "mini-kv/internal/raft"
	"mini-kv/internal/raft/logstore"
)

func TestNodeInit(t *testing.T) {
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

func TestLeaderElect(t *testing.T) {
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

	leader := waitLead(t, nodes, time.Second)
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

func TestLeaderFail(t *testing.T) {
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

	firstLeader := waitLeadMap(t, nodes, time.Second)
	if firstLeader == "" {
		t.Fatalf("first leader should be elected")
	}

	_ = nodes[firstLeader].Stop()
	transport.Unregister(firstLeader)

	delete(nodes, firstLeader)
	secondLeader := waitLeadMap(t, nodes, time.Second)
	if secondLeader == "" {
		t.Fatalf("new leader should be elected")
	}
	if secondLeader == firstLeader {
		t.Fatalf("new leader should differ from stopped leader")
	}
}

func waitLead(t *testing.T, nodes []raft.Node, timeout time.Duration) string {
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

func waitLeadMap(t *testing.T, nodes map[string]raft.Node, timeout time.Duration) string {
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

func TestPropose(t *testing.T) {
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

	leaderID := waitLeadMap(t, nodes, time.Second)
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

	msg := waitMsg(t, leader.ApplyCh(), time.Second)
	if msg.Index != index {
		t.Fatalf("apply index = %d, want %d", msg.Index, index)
	}
	if string(msg.Data) != "cmd-1" {
		t.Fatalf("apply data = %q, want cmd-1", msg.Data)
	}
}

func TestRejectPropose(t *testing.T) {
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

	leaderID := waitLeadMap(t, nodes, time.Second)
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

func waitMsg(t *testing.T, ch <-chan raft.ApplyMsg, timeout time.Duration) raft.ApplyMsg {
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
