package raft_test

import (
	"context"
	"sync"
	"testing"
	"time"

	raft "mini-kv/internal/raft"
	"mini-kv/internal/raft/logstore"
)

func TestReadIndexIsolated(t *testing.T) {
	net := newNet()
	nodes := newNodes(t, net, []string{"node1", "node2", "node3"})

	startNodes(t, nodes)
	defer stopNodes(nodes)

	leaderID := waitLeadMap(t, nodes, time.Second)
	if leaderID == "" {
		t.Fatalf("leader should be elected")
	}

	net.cut(leaderID)

	newLeaderID := waitOtherLead(t, nodes, leaderID, 2*time.Second)
	if newLeaderID == "" {
		t.Fatalf("majority side should elect new leader")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	if _, err := nodes[leaderID].ReadIndex(ctx); err == nil {
		t.Fatalf("isolated old leader read should fail")
	}
}

func TestReadIndexReady(t *testing.T) {
	net := newNet()
	nodes := newNodes(t, net, []string{"node1", "node2", "node3"})

	startNodes(t, nodes)
	defer stopNodes(nodes)

	leaderID := waitLeadMap(t, nodes, time.Second)
	if leaderID == "" {
		t.Fatalf("leader should be elected")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	index, err := nodes[leaderID].ReadIndex(ctx)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}
	if index == 0 {
		t.Fatalf("read index should be non-zero")
	}
}

func TestVoteStale(t *testing.T) {
	storage := logstore.NewMemoryStorage()
	if err := storage.Append([]raft.LogEntry{
		{Index: 1, Term: 2, Type: raft.EntryNormal, Data: []byte("newer")},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	node, err := raft.NewNode(raft.Config{
		ID:               "node1",
		Peers:            []string{"node1", "node2"},
		Storage:          storage,
		Transport:        raft.NewFakeTransport(),
		ElectionTimeout:  time.Second,
		HeartbeatTimeout: 100 * time.Millisecond,
		ApplyBufferSize:  16,
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	handler := node.(raft.RPCHandler)
	resp, err := handler.HandleRequestVote(context.Background(), raft.RequestVoteRequest{
		Term:         3,
		CandidateID:  "node2",
		LastLogIndex: 1,
		LastLogTerm:  1,
	})
	if err != nil {
		t.Fatalf("request vote: %v", err)
	}
	if resp.VoteGranted {
		t.Fatalf("stale candidate should not get vote")
	}
}

func TestConflictReplace(t *testing.T) {
	storage := logstore.NewMemoryStorage()
	if err := storage.Append([]raft.LogEntry{
		{Index: 1, Term: 1, Type: raft.EntryNormal, Data: []byte("a")},
		{Index: 2, Term: 9, Type: raft.EntryNormal, Data: []byte("bad")},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	node, err := raft.NewNode(raft.Config{
		ID:               "node1",
		Peers:            []string{"node1", "node2"},
		Storage:          storage,
		Transport:        raft.NewFakeTransport(),
		ElectionTimeout:  time.Second,
		HeartbeatTimeout: 100 * time.Millisecond,
		ApplyBufferSize:  16,
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	handler := node.(raft.RPCHandler)
	resp, err := handler.HandleAppendEntries(context.Background(), raft.AppendEntriesRequest{
		Term:         2,
		LeaderID:     "node2",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []raft.LogEntry{
			{Index: 2, Term: 2, Type: raft.EntryNormal, Data: []byte("good")},
		},
		LeaderCommit: 2,
	})
	if err != nil {
		t.Fatalf("append entries: %v", err)
	}
	if !resp.Success {
		t.Fatalf("append entries should succeed")
	}

	entries, err := storage.Entries(2, 3)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	if len(entries) != 1 || entries[0].Term != 2 || string(entries[0].Data) != "good" {
		t.Fatalf("entry = %+v, want term=2 data=good", entries)
	}
}

type partitionNetwork struct {
	mu       sync.RWMutex
	handlers map[string]raft.RPCHandler
	blocked  map[partitionLink]struct{}
}

type partitionLink struct {
	from string
	to   string
}

func newNet() *partitionNetwork {
	return &partitionNetwork{
		handlers: make(map[string]raft.RPCHandler),
		blocked:  make(map[partitionLink]struct{}),
	}
}

func (n *partitionNetwork) tr(id string) raft.Transport {
	return &partitionTransport{from: id, network: n}
}

func (n *partitionNetwork) add(id string, handler raft.RPCHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = handler
}

func (n *partitionNetwork) cut(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for peer := range n.handlers {
		if peer == id {
			continue
		}
		n.blocked[partitionLink{from: id, to: peer}] = struct{}{}
		n.blocked[partitionLink{from: peer, to: id}] = struct{}{}
	}
}

func (n *partitionNetwork) get(from string, to string) (raft.RPCHandler, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if _, ok := n.blocked[partitionLink{from: from, to: to}]; ok {
		return nil, raft.ErrNodeStopped
	}
	handler := n.handlers[to]
	if handler == nil {
		return nil, raft.ErrNodeStopped
	}
	return handler, nil
}

type partitionTransport struct {
	from    string
	network *partitionNetwork
}

func (t *partitionTransport) RequestVote(ctx context.Context, target string, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	handler, err := t.network.get(t.from, target)
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}
	return handler.HandleRequestVote(ctx, req)
}

func (t *partitionTransport) AppendEntries(ctx context.Context, target string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	handler, err := t.network.get(t.from, target)
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	return handler.HandleAppendEntries(ctx, req)
}

func (t *partitionTransport) InstallSnapshot(ctx context.Context, target string, req raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	handler, err := t.network.get(t.from, target)
	if err != nil {
		return raft.InstallSnapshotResponse{}, err
	}
	return handler.HandleInstallSnapshot(ctx, req)
}

func newNodes(t *testing.T, net *partitionNetwork, ids []string) map[string]raft.Node {
	t.Helper()

	nodes := make(map[string]raft.Node, len(ids))
	for _, id := range ids {
		node, err := raft.NewNode(raft.Config{
			ID:               id,
			Peers:            ids,
			Storage:          logstore.NewMemoryStorage(),
			Transport:        net.tr(id),
			ElectionTimeout:  80 * time.Millisecond,
			HeartbeatTimeout: 20 * time.Millisecond,
			ApplyBufferSize:  16,
		})
		if err != nil {
			t.Fatalf("new node %s: %v", id, err)
		}
		net.add(id, node.(raft.RPCHandler))
		nodes[id] = node
	}
	return nodes
}

func startNodes(t *testing.T, nodes map[string]raft.Node) {
	t.Helper()
	for _, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("start node: %v", err)
		}
	}
}

func stopNodes(nodes map[string]raft.Node) {
	for _, node := range nodes {
		_ = node.Stop()
	}
}

func waitOtherLead(t *testing.T, nodes map[string]raft.Node, excluded string, timeout time.Duration) string {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id, node := range nodes {
			if id == excluded {
				continue
			}
			if node.IsLeader() {
				return id
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return ""
}
