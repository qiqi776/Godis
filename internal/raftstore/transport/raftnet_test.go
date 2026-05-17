package transport

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"mini-kv/internal/raft"
	"mini-kv/internal/raft/logstore"
)

func TestRoundTrip(t *testing.T) {
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

func TestAppendEntriesReusesConnection(t *testing.T) {
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
	defer client.Close()

	for i := 0; i < 10; i++ {
		resp, err := client.AppendEntries(context.Background(), "node1", raft.AppendEntriesRequest{
			Term:     uint64(i + 1),
			LeaderID: "node2",
		})
		if err != nil {
			t.Fatalf("append entries %d: %v", i, err)
		}
		if resp.Term != uint64(i+1) || !resp.Success {
			t.Fatalf("append response %d = %+v", i, resp)
		}
	}
	if accepted := server.acceptedConnCount(); accepted != 1 {
		t.Fatalf("accepted connections = %d, want 1", accepted)
	}
}

func TestConcurrentAppendEntries(t *testing.T) {
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
	defer client.Close()

	const goroutines = 16
	const callsPerGoroutine = 20
	errCh := make(chan error, goroutines*callsPerGoroutine)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < callsPerGoroutine; j++ {
				term := uint64(id*callsPerGoroutine + j + 1)
				resp, err := client.AppendEntries(context.Background(), "node1", raft.AppendEntriesRequest{
					Term:     term,
					LeaderID: "node2",
				})
				if err != nil {
					errCh <- err
					continue
				}
				if resp.Term != term || !resp.Success {
					errCh <- errors.New("unexpected append response")
				}
			}
		}(i)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("append entries: %v", err)
		}
	}
	if accepted := server.acceptedConnCount(); accepted == 0 || accepted > peerConnPoolSize {
		t.Fatalf("accepted connections = %d, want 1..%d", accepted, peerConnPoolSize)
	}
}

func TestSetPeerClosesOldConnection(t *testing.T) {
	first, err := New("node1", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("new first server: %v", err)
	}
	if err := first.Start(&termHandler{term: 11}); err != nil {
		t.Fatalf("start first server: %v", err)
	}
	defer first.Close()

	second, err := New("node3", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("new second server: %v", err)
	}
	if err := second.Start(&termHandler{term: 22}); err != nil {
		t.Fatalf("start second server: %v", err)
	}
	defer second.Close()

	client, err := New("node2", "127.0.0.1:0", map[string]string{
		"node1": first.Addr(),
	})
	if err != nil {
		t.Fatalf("new client transport: %v", err)
	}
	defer client.Close()

	resp, err := client.RequestVote(context.Background(), "node1", raft.RequestVoteRequest{})
	if err != nil {
		t.Fatalf("request vote first: %v", err)
	}
	if resp.Term != 11 {
		t.Fatalf("first response term = %d, want 11", resp.Term)
	}

	client.SetPeer("node1", second.Addr())
	resp, err = client.RequestVote(context.Background(), "node1", raft.RequestVoteRequest{})
	if err != nil {
		t.Fatalf("request vote second: %v", err)
	}
	if resp.Term != 22 {
		t.Fatalf("second response term = %d, want 22", resp.Term)
	}
}

func TestCloseRejectsNewCalls(t *testing.T) {
	client, err := New("node2", "127.0.0.1:0", map[string]string{
		"node1": "127.0.0.1:1",
	})
	if err != nil {
		t.Fatalf("new client transport: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}
	_, err = client.AppendEntries(context.Background(), "node1", raft.AppendEntriesRequest{})
	if !errors.Is(err, ErrTransportClosed) {
		t.Fatalf("append after close error = %v, want %v", err, ErrTransportClosed)
	}
}

func TestCluster(t *testing.T) {
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

	leaderID := waitLead(t, nodes, 2*time.Second)
	index, err := nodes[leaderID].Propose(context.Background(), []byte("net-cmd"))
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	if index == 0 {
		t.Fatalf("index should be non-zero")
	}

	for id, node := range nodes {
		msg := waitMsg(t, node.ApplyCh(), []byte("net-cmd"), 2*time.Second)
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

type termHandler struct {
	term uint64
}

func (s *termHandler) HandleRequestVote(ctx context.Context, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	return raft.RequestVoteResponse{Term: s.term, VoteGranted: true}, nil
}

func (s *termHandler) HandleAppendEntries(ctx context.Context, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	return raft.AppendEntriesResponse{Term: s.term, Success: true}, nil
}

func (s *termHandler) HandleInstallSnapshot(ctx context.Context, req raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	return raft.InstallSnapshotResponse{Term: s.term}, nil
}

func waitLead(t *testing.T, nodes map[string]raft.Node, timeout time.Duration) string {
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

func waitMsg(t *testing.T, ch <-chan raft.ApplyMsg, data []byte, timeout time.Duration) raft.ApplyMsg {
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
