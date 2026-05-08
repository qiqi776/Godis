package transport

import (
	"bytes"
	"context"
	"testing"
	"time"

	"mini-kv/internal/raft"
	"mini-kv/internal/raft/logstore"
)

func TestTransportRoundTrip(t *testing.T) {
	handler := &stubHandler{}
	server, err := New("node1", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("new server transport: %v", err)
	}
	if err := server.Start(handler); err != nil {
		t.Fatalf("start server transport: %v", err)
	}
	defer server.Close()

	client, err := New("node2", "127.0.0.1:0", map[string]string{
		"node1": server.Addr(),
	})
	if err != nil {
		t.Fatalf("new client transport: %v", err)
	}

	resp, err := client.RequestVote(context.Background(), "node1", raft.RequestVoteRequest{
		Term:        3,
		CandidateID: "node2",
	})
	if err != nil {
		t.Fatalf("request vote: %v", err)
	}
	if resp.Term != 3 || !resp.VoteGranted {
		t.Fatalf("vote resp = %+v", resp)
	}
}

func TestRealTransportCluster(t *testing.T) {
	ids := []string{"node1", "node2", "node3"}
	transports := make(map[string]*Transport, len(ids))
	nodes := make(map[string]raft.Node, len(ids))

	for _, id := range ids {
		tr, err := New(id, "127.0.0.1:0", nil)
		if err != nil {
			t.Fatalf("new transport %s: %v", id, err)
		}
		transports[id] = tr
	}

	for _, id := range ids {
		node, err := raft.NewNode(raft.Config{
			ID:               id,
			Peers:            ids,
			Storage:          logstore.NewMemoryStorage(),
			Transport:        transports[id],
			ElectionTimeout:  80 * time.Millisecond,
			HeartbeatTimeout: 20 * time.Millisecond,
			ApplyBufferSize:  16,
		})
		if err != nil {
			t.Fatalf("new node %s: %v", id, err)
		}
		handler := node.(raft.RPCHandler)
		if err := transports[id].Start(handler); err != nil {
			t.Fatalf("start transport %s: %v", id, err)
		}
		nodes[id] = node
	}

	for _, tr := range transports {
		for peer, peerTr := range transports {
			tr.SetPeer(peer, peerTr.Addr())
		}
	}

	for _, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("start node: %v", err)
		}
	}
	defer func() {
		for _, node := range nodes {
			_ = node.Stop()
		}
		for _, tr := range transports {
			_ = tr.Close()
		}
	}()

	leaderID := waitNetLeader(t, nodes, 2*time.Second)
	index, err := nodes[leaderID].Propose(context.Background(), []byte("net-cmd"))
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	if index == 0 {
		t.Fatalf("index should be non-zero")
	}

	for id, node := range nodes {
		msg := waitNetApply(t, node.ApplyCh(), []byte("net-cmd"), 2*time.Second)
		if msg.Index != index {
			t.Fatalf("node=%s index=%d want=%d", id, msg.Index, index)
		}
	}
}

type stubHandler struct{}

func (s *stubHandler) HandleRequestVote(ctx context.Context, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	return raft.RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
}

func (s *stubHandler) HandleAppendEntries(ctx context.Context, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	return raft.AppendEntriesResponse{Term: req.Term, Success: true}, nil
}

func (s *stubHandler) HandleInstallSnapshot(ctx context.Context, req raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	return raft.InstallSnapshotResponse{Term: req.Term}, nil
}

func waitNetLeader(t *testing.T, nodes map[string]raft.Node, timeout time.Duration) string {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leader string
		count := 0
		for id, node := range nodes {
			if node.IsLeader() {
				leader = id
				count++
			}
		}
		if count == 1 {
			return leader
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for leader")
	return ""
}

func waitNetApply(t *testing.T, ch <-chan raft.ApplyMsg, data []byte, timeout time.Duration) raft.ApplyMsg {
	t.Helper()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case msg := <-ch:
			if msg.Type == raft.EntryNoop {
				continue
			}
			if bytes.Equal(msg.Data, data) {
				return msg
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for apply %q", data)
			return raft.ApplyMsg{}
		}
	}
}
