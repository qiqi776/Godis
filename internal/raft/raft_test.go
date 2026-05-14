package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestNoSelf(t *testing.T) {
	_, err := NewNode(Config{
		ID:               "node1",
		Peers:            []string{"node2", "node3"},
		Storage:          &failingHardStateStorage{},
		Transport:        NewFakeTransport(),
		ElectionTimeout:  time.Second,
		HeartbeatTimeout: 100 * time.Millisecond,
		ApplyBufferSize:  16,
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("new node error = %v, want %v", err, ErrInvalidConfig)
	}
}

func TestDupPeer(t *testing.T) {
	_, err := NewNode(Config{
		ID:               "node1",
		Peers:            []string{"node1", "node2", "node2"},
		Storage:          &failingHardStateStorage{},
		Transport:        NewFakeTransport(),
		ElectionTimeout:  time.Second,
		HeartbeatTimeout: 100 * time.Millisecond,
		ApplyBufferSize:  16,
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("new node error = %v, want %v", err, ErrInvalidConfig)
	}
}

func TestFake(t *testing.T) {
	transport := NewFakeTransport()
	storage := newMemStorage()

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
	if resp.Term != 1 {
		t.Fatalf("term = %d, want 1", resp.Term)
	}
	if !resp.VoteGranted {
		t.Fatalf("vote should be granted")
	}
}

func TestNodeInit(t *testing.T) {
	storage := newMemStorage()
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

func TestLeaderElect(t *testing.T) {
	transport := NewFakeTransport()
	peers := []string{"node1", "node2", "node3"}

	nodes := make([]Node, 0, len(peers))
	for _, id := range peers {
		node, err := NewNode(Config{
			ID:               id,
			Peers:            peers,
			Storage:          newMemStorage(),
			Transport:        transport,
			ElectionTimeout:  80 * time.Millisecond,
			HeartbeatTimeout: 20 * time.Millisecond,
			ApplyBufferSize:  16,
		})
		if err != nil {
			t.Fatalf("new node %s error: %v", id, err)
		}

		handler, ok := node.(RPCHandler)
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
	transport := NewFakeTransport()
	peers := []string{"node1", "node2", "node3"}

	nodes := make(map[string]Node)
	for _, id := range peers {
		node, err := NewNode(Config{
			ID:               id,
			Peers:            peers,
			Storage:          newMemStorage(),
			Transport:        transport,
			ElectionTimeout:  80 * time.Millisecond,
			HeartbeatTimeout: 20 * time.Millisecond,
			ApplyBufferSize:  16,
		})
		if err != nil {
			t.Fatalf("new node %s error: %v", id, err)
		}

		handler, ok := node.(RPCHandler)
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

func TestPropose(t *testing.T) {
	transport := NewFakeTransport()
	peers := []string{"node1", "node2", "node3"}

	nodes := make(map[string]Node)
	for _, id := range peers {
		node, err := NewNode(Config{
			ID:               id,
			Peers:            peers,
			Storage:          newMemStorage(),
			Transport:        transport,
			ElectionTimeout:  80 * time.Millisecond,
			HeartbeatTimeout: 20 * time.Millisecond,
			ApplyBufferSize:  16,
		})
		if err != nil {
			t.Fatalf("new node %s error: %v", id, err)
		}

		handler, ok := node.(RPCHandler)
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
	transport := NewFakeTransport()
	peers := []string{"node1", "node2", "node3"}

	nodes := make(map[string]Node)
	for _, id := range peers {
		node, err := NewNode(Config{
			ID:               id,
			Peers:            peers,
			Storage:          newMemStorage(),
			Transport:        transport,
			ElectionTimeout:  80 * time.Millisecond,
			HeartbeatTimeout: 20 * time.Millisecond,
			ApplyBufferSize:  16,
		})
		if err != nil {
			t.Fatalf("new node %s error: %v", id, err)
		}

		handler, ok := node.(RPCHandler)
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
		if err != ErrNotLeader {
			t.Fatalf("follower propose error = %v, want %v", err, ErrNotLeader)
		}
		return
	}

	t.Fatalf("no follower found")
}

func TestProposeBatchesConcurrentEntries(t *testing.T) {
	previousWindow := proposalBatchWindow
	proposalBatchWindow = 10 * time.Millisecond
	t.Cleanup(func() {
		proposalBatchWindow = previousWindow
	})

	storage := &batchCountingStorage{memStorage: newMemStorage()}
	node := newTestNode(t, "node1", storage, NewFakeTransport())
	defer node.Stop()
	_ = becomeLeader(node, 1)

	const proposals = 16
	start := make(chan struct{})
	errCh := make(chan error, proposals)

	var wg sync.WaitGroup
	wg.Add(proposals)
	for i := 0; i < proposals; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			index, err := node.Propose(context.Background(), []byte(fmt.Sprintf("cmd-%d", i)))
			if err != nil {
				errCh <- err
				return
			}
			if index == 0 {
				errCh <- fmt.Errorf("proposal index is zero")
			}
		}()
	}

	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("propose: %v", err)
		}
	}

	if maxBatch := storage.maxBatchSize(); maxBatch < 2 {
		t.Fatalf("max append batch size = %d, want at least 2", maxBatch)
	}

	entries, err := storage.Entries(1, proposals+1)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	if len(entries) != proposals {
		t.Fatalf("entry count = %d, want %d", len(entries), proposals)
	}
}

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

func TestReadIndexWithLaggingFollowerUsesHeartbeatPath(t *testing.T) {
	transport := NewFakeTransport()

	leaderStorage := newMemStorage()
	if err := leaderStorage.Append([]LogEntry{
		{Index: 1, Term: 2, Type: EntryNormal, Data: []byte("a")},
		{Index: 2, Term: 2, Type: EntryNormal, Data: []byte("b")},
		{Index: 3, Term: 2, Type: EntryNormal, Data: []byte("c")},
	}); err != nil {
		t.Fatalf("append leader entries: %v", err)
	}
	leader, err := NewNode(Config{
		ID:               "node1",
		Peers:            []string{"node1", "node2"},
		Storage:          leaderStorage,
		Transport:        transport,
		ElectionTimeout:  time.Second,
		HeartbeatTimeout: 100 * time.Millisecond,
		ApplyBufferSize:  16,
	})
	if err != nil {
		t.Fatalf("new leader: %v", err)
	}

	follower, err := NewNode(Config{
		ID:               "node2",
		Peers:            []string{"node1", "node2"},
		Storage:          newMemStorage(),
		Transport:        transport,
		ElectionTimeout:  time.Second,
		HeartbeatTimeout: 100 * time.Millisecond,
		ApplyBufferSize:  16,
	})
	if err != nil {
		t.Fatalf("new follower: %v", err)
	}
	transport.Register("node2", follower.(RPCHandler))

	leaderNode := leader.(*raftNode)
	leaderNode.state = Leader
	leaderNode.currentTerm = 2
	leaderNode.votedFor = leaderNode.id
	leaderNode.leaderID = leaderNode.id
	leaderNode.commitIndex = 3
	leaderNode.commitTerm = 2
	leaderNode.lastApplied = 3
	leaderNode.nextIndex["node2"] = 4
	leaderNode.matchIndex["node1"] = 3

	followerNode := follower.(*raftNode)
	followerNode.currentTerm = 2

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	index, err := leader.ReadIndex(ctx)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}
	if index != 3 {
		t.Fatalf("read index = %d, want 3", index)
	}
}

func TestReadIndexCoalescesConcurrentConfirms(t *testing.T) {
	storage := newMemStorage()
	appendTerms(t, storage, 1)

	transport := newBlockingReadTransport()
	node := newTestNode(t, "node1", storage, transport)
	defer node.Stop()

	raftNode := becomeLeader(node, 1)
	raftNode.commitIndex = 1
	raftNode.commitTerm = 1
	raftNode.lastApplied = 1
	raftNode.matchIndex["node1"] = 1

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	errCh := make(chan error, 2)
	go func() {
		_, err := node.ReadIndex(ctx)
		errCh <- err
	}()

	transport.waitStarted(t, time.Second)

	go func() {
		_, err := node.ReadIndex(ctx)
		errCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
	transport.release()

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("read index error: %v", err)
		}
	}

	if calls := transport.callCount(); calls != 1 {
		t.Fatalf("read confirm append calls = %d, want 1", calls)
	}
}

func TestReadIndexBatchWindowCoalescesBeforeConfirm(t *testing.T) {
	previousWindow := readConfirmBatchWindow
	readConfirmBatchWindow = 20 * time.Millisecond
	defer func() {
		readConfirmBatchWindow = previousWindow
	}()

	storage := newMemStorage()
	appendTerms(t, storage, 1)

	transport := &countingReadTransport{}
	node := newTestNode(t, "node1", storage, transport)
	defer node.Stop()

	raftNode := becomeLeader(node, 1)
	raftNode.commitIndex = 1
	raftNode.commitTerm = 1
	raftNode.lastApplied = 1
	raftNode.matchIndex["node1"] = 1

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	errCh := make(chan error, 2)
	go func() {
		_, err := node.ReadIndex(ctx)
		errCh <- err
	}()
	time.Sleep(2 * time.Millisecond)
	go func() {
		_, err := node.ReadIndex(ctx)
		errCh <- err
	}()

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("read index error: %v", err)
		}
	}

	if calls := transport.callCount(); calls != 1 {
		t.Fatalf("read confirm append calls = %d, want 1", calls)
	}
}

func TestVoteStale(t *testing.T) {
	storage := newMemStorage()
	if err := storage.Append([]LogEntry{
		{Index: 1, Term: 2, Type: EntryNormal, Data: []byte("newer")},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	node, err := NewNode(Config{
		ID:               "node1",
		Peers:            []string{"node1", "node2"},
		Storage:          storage,
		Transport:        NewFakeTransport(),
		ElectionTimeout:  time.Second,
		HeartbeatTimeout: 100 * time.Millisecond,
		ApplyBufferSize:  16,
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	handler := node.(RPCHandler)
	resp, err := handler.HandleRequestVote(context.Background(), RequestVoteRequest{
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
	storage := newMemStorage()
	if err := storage.Append([]LogEntry{
		{Index: 1, Term: 1, Type: EntryNormal, Data: []byte("a")},
		{Index: 2, Term: 9, Type: EntryNormal, Data: []byte("bad")},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	node, err := NewNode(Config{
		ID:               "node1",
		Peers:            []string{"node1", "node2"},
		Storage:          storage,
		Transport:        NewFakeTransport(),
		ElectionTimeout:  time.Second,
		HeartbeatTimeout: 100 * time.Millisecond,
		ApplyBufferSize:  16,
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	handler := node.(RPCHandler)
	resp, err := handler.HandleAppendEntries(context.Background(), AppendEntriesRequest{
		Term:         2,
		LeaderID:     "node2",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Index: 2, Term: 2, Type: EntryNormal, Data: []byte("good")},
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

func TestElectRollback(t *testing.T) {
	node := newFailNode(t)
	node.state = Follower
	node.currentTerm = 7
	node.leaderID = "node2"

	node.Election()

	assertState(t, node, Follower, 7, "", "node2")
}

func TestVoteTermRollback(t *testing.T) {
	node := newFailNode(t)
	node.state = Leader
	node.currentTerm = 4
	node.votedFor = node.id
	node.leaderID = node.id

	_, err := node.HandleRequestVote(context.Background(), RequestVoteRequest{
		Term:         5,
		CandidateID:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if !errors.Is(err, errInjectedHardState) {
		t.Fatalf("request vote error = %v, want %v", err, errInjectedHardState)
	}

	assertState(t, node, Follower, 5, "", "")
	assertFatalStop(t, node)
}

func TestVoteGrantRollback(t *testing.T) {
	node := newFailNode(t)
	node.state = Follower
	node.currentTerm = 4
	node.leaderID = "node9"

	_, err := node.HandleRequestVote(context.Background(), RequestVoteRequest{
		Term:         4,
		CandidateID:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if !errors.Is(err, errInjectedHardState) {
		t.Fatalf("request vote error = %v, want %v", err, errInjectedHardState)
	}

	assertState(t, node, Follower, 4, "", "node9")
}

func TestAppendTermRollback(t *testing.T) {
	node := newFailNode(t)
	node.state = Candidate
	node.currentTerm = 4
	node.votedFor = node.id

	_, err := node.HandleAppendEntries(context.Background(), AppendEntriesRequest{
		Term:         5,
		LeaderID:     "node2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	})
	if !errors.Is(err, errInjectedHardState) {
		t.Fatalf("append entries error = %v, want %v", err, errInjectedHardState)
	}
	assertState(t, node, Follower, 5, "", "")
	assertFatalStop(t, node)
}

func TestSnapTermRollback(t *testing.T) {
	node := newFailNode(t)
	node.state = Candidate
	node.currentTerm = 4
	node.votedFor = node.id

	_, err := node.HandleInstallSnapshot(context.Background(), InstallSnapshotRequest{
		Term:              5,
		LeaderID:          "node2",
		LastIncludedIndex: 1,
		LastIncludedTerm:  1,
		Data:              []byte("snapshot"),
	})
	if !errors.Is(err, errInjectedHardState) {
		t.Fatalf("install snapshot error = %v, want %v", err, errInjectedHardState)
	}
	assertState(t, node, Follower, 5, "", "")
	assertFatalStop(t, node)
}

func TestStepDownRollback(t *testing.T) {
	node := newFailNode(t)
	node.state = Leader
	node.currentTerm = 4
	node.votedFor = node.id
	node.leaderID = node.id

	err := node.stepDown(5, "node2")
	if !errors.Is(err, errInjectedHardState) {
		t.Fatalf("step down error = %v, want %v", err, errInjectedHardState)
	}

	assertState(t, node, Follower, 5, "", "")
	assertFatalStop(t, node)

	if _, err := node.Propose(context.Background(), []byte("cmd")); !errors.Is(err, ErrNodeStopped) {
		t.Fatalf("propose after fatal stop error = %v, want %v", err, ErrNodeStopped)
	}
	if !errors.Is(node.fatalErr, errInjectedHardState) {
		t.Fatalf("fatalErr = %v, want %v", node.fatalErr, errInjectedHardState)
	}
}

func TestReadIndexRejectedAfterFatalStepDown(t *testing.T) {
	node := newFailNode(t)
	node.state = Leader
	node.currentTerm = 4
	node.votedFor = node.id
	node.leaderID = node.id

	err := node.stepDown(5, "node2")
	if !errors.Is(err, errInjectedHardState) {
		t.Fatalf("step down error = %v, want %v", err, errInjectedHardState)
	}

	_, err = node.ReadIndex(context.Background())
	if !errors.Is(err, ErrNodeStopped) {
		t.Fatalf("read index after fatal stop error = %v, want %v", err, ErrNodeStopped)
	}
	if !errors.Is(err, errInjectedHardState) {
		t.Fatalf("read index after fatal stop error = %v, want %v", err, errInjectedHardState)
	}
}

func TestStopClosesApplyCh(t *testing.T) {
	node, err := NewNode(Config{
		ID:               "node1",
		Peers:            []string{"node1"},
		Storage:          newMemStorage(),
		Transport:        NewFakeTransport(),
		ElectionTimeout:  time.Second,
		HeartbeatTimeout: 100 * time.Millisecond,
		ApplyBufferSize:  1,
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	if err := node.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}

	select {
	case _, ok := <-node.ApplyCh():
		if ok {
			t.Fatal("applyCh should be closed after stop")
		}
	case <-time.After(time.Second):
		t.Fatal("applyCh was not closed after stop")
	}
}

func TestStopClosesApplyChWithPendingSnapshot(t *testing.T) {
	storage := newMemStorage()
	storage.snapshot = Snapshot{
		Index: 1,
		Term:  1,
		Data:  []byte("snapshot"),
	}

	node, err := NewNode(Config{
		ID:               "node1",
		Peers:            []string{"node1"},
		Storage:          storage,
		Transport:        NewFakeTransport(),
		ElectionTimeout:  time.Second,
		HeartbeatTimeout: 100 * time.Millisecond,
		ApplyBufferSize:  1,
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	raftNode := node.(*raftNode)
	raftNode.applyCh <- ApplyMsg{Index: 99}

	if err := node.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	if err := node.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}

	msg, ok := <-node.ApplyCh()
	if !ok {
		t.Fatal("expected buffered message before applyCh close")
	}
	if msg.Index != 99 {
		t.Fatalf("buffered msg index = %d, want 99", msg.Index)
	}

	select {
	case _, ok := <-node.ApplyCh():
		if ok {
			t.Fatal("applyCh should be closed after stop")
		}
	case <-time.After(time.Second):
		t.Fatal("applyCh was not closed after stop")
	}
}

func TestSnapshotCommitAdvanceDoesNotPersistHardState(t *testing.T) {
	storage := &failCommitStorage{memStorage: newMemStorage()}
	node, err := NewNode(Config{
		ID:               "node1",
		Peers:            []string{"node1", "node2"},
		Storage:          storage,
		Transport:        NewFakeTransport(),
		ElectionTimeout:  time.Second,
		HeartbeatTimeout: 100 * time.Millisecond,
		ApplyBufferSize:  16,
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	raftNode := node.(*raftNode)
	raftNode.currentTerm = 1
	storage.fail = true

	resp, err := raftNode.HandleInstallSnapshot(context.Background(), InstallSnapshotRequest{
		Term:              1,
		LeaderID:          "node2",
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
		Data:              []byte("snapshot"),
	})
	if err != nil {
		t.Fatalf("install snapshot: %v", err)
	}
	if resp.Term != 1 {
		t.Fatalf("snapshot response term = %d, want 1", resp.Term)
	}
	if raftNode.commitIndex != 5 {
		t.Fatalf("commitIndex = %d, want 5", raftNode.commitIndex)
	}
	if raftNode.lastApplied != 5 {
		t.Fatalf("lastApplied = %d, want 5", raftNode.lastApplied)
	}
	if raftNode.restoreSnapshot.Index != 5 {
		t.Fatalf("restoreSnapshot = %+v, want index 5", raftNode.restoreSnapshot)
	}
	if raftNode.stopped {
		t.Fatal("node stopped after commit-only hard state failure")
	}
}

func waitLead(t *testing.T, nodes []Node, timeout time.Duration) string {
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

func waitLeadMap(t *testing.T, nodes map[string]Node, timeout time.Duration) string {
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

func waitMsg(t *testing.T, ch <-chan ApplyMsg, timeout time.Duration) ApplyMsg {
	t.Helper()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				t.Fatalf("apply channel closed")
			}
			if msg.Type == EntryNoop {
				continue
			}
			return msg
		case <-timer.C:
			t.Fatalf("timed out waiting for apply")
			return ApplyMsg{}
		}
	}
}

type partitionNetwork struct {
	mu       sync.RWMutex
	handlers map[string]RPCHandler
	blocked  map[partitionLink]struct{}
}

type partitionLink struct {
	from string
	to   string
}

func newNet() *partitionNetwork {
	return &partitionNetwork{
		handlers: make(map[string]RPCHandler),
		blocked:  make(map[partitionLink]struct{}),
	}
}

func (n *partitionNetwork) tr(id string) Transport {
	return &partitionTransport{from: id, network: n}
}

func (n *partitionNetwork) add(id string, handler RPCHandler) {
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

func (n *partitionNetwork) heal(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for link := range n.blocked {
		if link.from == id || link.to == id {
			delete(n.blocked, link)
		}
	}
}

func (n *partitionNetwork) get(from string, to string) (RPCHandler, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if _, ok := n.blocked[partitionLink{from: from, to: to}]; ok {
		return nil, ErrNodeStopped
	}
	handler := n.handlers[to]
	if handler == nil {
		return nil, ErrNodeStopped
	}
	return handler, nil
}

type partitionTransport struct {
	from    string
	network *partitionNetwork
}

func (t *partitionTransport) RequestVote(ctx context.Context, target string, req RequestVoteRequest) (RequestVoteResponse, error) {
	handler, err := t.network.get(t.from, target)
	if err != nil {
		return RequestVoteResponse{}, err
	}
	return handler.HandleRequestVote(ctx, req)
}

func (t *partitionTransport) AppendEntries(ctx context.Context, target string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	handler, err := t.network.get(t.from, target)
	if err != nil {
		return AppendEntriesResponse{}, err
	}
	return handler.HandleAppendEntries(ctx, req)
}

func (t *partitionTransport) InstallSnapshot(ctx context.Context, target string, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	handler, err := t.network.get(t.from, target)
	if err != nil {
		return InstallSnapshotResponse{}, err
	}
	return handler.HandleInstallSnapshot(ctx, req)
}

func newNodes(t *testing.T, net *partitionNetwork, ids []string) map[string]Node {
	t.Helper()

	nodes := make(map[string]Node, len(ids))
	for _, id := range ids {
		node, err := NewNode(Config{
			ID:               id,
			Peers:            ids,
			Storage:          newMemStorage(),
			Transport:        net.tr(id),
			ElectionTimeout:  80 * time.Millisecond,
			HeartbeatTimeout: 20 * time.Millisecond,
			ApplyBufferSize:  16,
		})
		if err != nil {
			t.Fatalf("new node %s: %v", id, err)
		}
		net.add(id, node.(RPCHandler))
		nodes[id] = node
	}
	return nodes
}

func startNodes(t *testing.T, nodes map[string]Node) {
	t.Helper()
	for _, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("start node: %v", err)
		}
	}
}

func stopNodes(nodes map[string]Node) {
	for _, node := range nodes {
		_ = node.Stop()
	}
}

func waitOtherLead(t *testing.T, nodes map[string]Node, excluded string, timeout time.Duration) string {
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

func TestAppendEntriesRejectReportsConflictHint(t *testing.T) {
	tests := []struct {
		name         string
		terms        []uint64
		prevLogIndex uint64
		prevLogTerm  uint64
		wantIndex    uint64
		wantTerm     uint64
		requestTerm  uint64
	}{
		{
			name:         "missing entry",
			terms:        []uint64{1, 1},
			prevLogIndex: 4,
			prevLogTerm:  2,
			wantIndex:    3,
			requestTerm:  2,
		},
		{
			name:         "term mismatch",
			terms:        []uint64{1, 2, 2, 2, 3},
			prevLogIndex: 4,
			prevLogTerm:  9,
			wantIndex:    2,
			wantTerm:     2,
			requestTerm:  4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := newMemStorage()
			appendTerms(t, storage, tt.terms...)
			node := newTestNode(t, "node1", storage, NewFakeTransport())

			resp, err := node.(RPCHandler).HandleAppendEntries(context.Background(), AppendEntriesRequest{
				Term:         tt.requestTerm,
				LeaderID:     "node2",
				PrevLogIndex: tt.prevLogIndex,
				PrevLogTerm:  tt.prevLogTerm,
			})
			if err != nil {
				t.Fatalf("append entries: %v", err)
			}
			if resp.Success {
				t.Fatal("append entries should fail")
			}
			if resp.ConflictIndex != tt.wantIndex || resp.ConflictTerm != tt.wantTerm {
				t.Fatalf("conflict hint = (%d,%d), want (%d,%d)",
					resp.ConflictIndex, resp.ConflictTerm, tt.wantIndex, tt.wantTerm)
			}
		})
	}
}

func TestDecreaseNextIndexUsesConflictHint(t *testing.T) {
	tests := []struct {
		name      string
		terms     []uint64
		term      uint64
		nextIndex uint64
		resp      AppendEntriesResponse
		wantNext  uint64
	}{
		{
			name:      "skip known conflict term",
			terms:     []uint64{1, 2, 2, 3, 4, 4, 5},
			term:      5,
			nextIndex: 8,
			resp:      AppendEntriesResponse{ConflictIndex: 5, ConflictTerm: 4},
			wantNext:  7,
		},
		{
			name:      "use conflict index when term is absent",
			terms:     []uint64{1, 2, 3, 4},
			term:      4,
			nextIndex: 5,
			resp:      AppendEntriesResponse{ConflictIndex: 2, ConflictTerm: 9},
			wantNext:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := newMemStorage()
			appendTerms(t, storage, tt.terms...)
			raftNode := newLeaderNode(t, storage, tt.term)
			raftNode.nextIndex["node2"] = tt.nextIndex

			if ok := raftNode.decreaseNextIndex("node2", tt.term, tt.resp); !ok {
				t.Fatal("decreaseNextIndex should continue replication")
			}
			if raftNode.nextIndex["node2"] != tt.wantNext {
				t.Fatalf("nextIndex = %d, want %d", raftNode.nextIndex["node2"], tt.wantNext)
			}
		})
	}
}

func TestReplicationWorkerCoalescesInflightRequests(t *testing.T) {
	storage := newMemStorage()
	appendTerms(t, storage, 1)

	transport := newBlockingAppendTransport()
	node := newTestNode(t, "node1", storage, transport)
	defer node.Stop()

	raftNode := becomeLeader(node, 1)
	raftNode.nextIndex["node2"] = 1
	raftNode.matchIndex["node1"] = 1
	startReplicationWorker(raftNode, "node2")

	raftNode.notifyReplication("node2")
	raftNode.notifyReplication("node2")
	raftNode.notifyReplication("node2")

	transport.waitStarted(t, time.Second)
	time.Sleep(20 * time.Millisecond)

	calls, maxConcurrent := transport.stats()
	if calls != 1 {
		t.Fatalf("append call count while inflight = %d, want 1", calls)
	}
	if maxConcurrent != 1 {
		t.Fatalf("max concurrent append calls = %d, want 1", maxConcurrent)
	}

	transport.release()
}

func TestReplicationWorkerBatchesBacklog(t *testing.T) {
	leaderStorage := newMemStorage()
	entryCount := replicationBatchSize*2 + 7
	appendRepeatedTerm(t, leaderStorage, entryCount, 1)

	followerStorage := newMemStorage()
	baseTransport := NewFakeTransport()
	follower := newTestNode(t, "node2", followerStorage, baseTransport)
	defer follower.Stop()
	baseTransport.Register("node2", follower.(RPCHandler))

	transport := &recordingTransport{delegate: baseTransport}
	leader := newTestNode(t, "node1", leaderStorage, transport)
	defer leader.Stop()

	leaderNode := becomeLeader(leader, 1)
	leaderNode.nextIndex["node2"] = 1
	leaderNode.matchIndex["node1"] = uint64(entryCount)
	startReplicationWorker(leaderNode, "node2")

	leaderNode.notifyReplication("node2")

	waitForCondition(t, time.Second, func() bool {
		lastIndex, err := followerStorage.LastIndex()
		return err == nil && lastIndex == uint64(entryCount)
	})

	batches := transport.entryBatchSizes()
	positiveBatches := 0
	for _, batchSize := range batches {
		if batchSize == 0 {
			continue
		}
		positiveBatches++
		if batchSize > replicationBatchSize {
			t.Fatalf("batch size = %d, want <= %d", batchSize, replicationBatchSize)
		}
	}
	if positiveBatches < 3 {
		t.Fatalf("positive batch count = %d, want at least 3", positiveBatches)
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not satisfied before timeout")
}

func newTestNode(t *testing.T, id string, storage Storage, transport Transport) Node {
	t.Helper()

	node, err := NewNode(Config{
		ID:               id,
		Peers:            []string{"node1", "node2"},
		Storage:          storage,
		Transport:        transport,
		ElectionTimeout:  time.Second,
		HeartbeatTimeout: 100 * time.Millisecond,
		ApplyBufferSize:  16,
	})
	if err != nil {
		t.Fatalf("new node %s: %v", id, err)
	}
	return node
}

func newLeaderNode(t *testing.T, storage Storage, term uint64) *raftNode {
	t.Helper()
	return becomeLeader(newTestNode(t, "node1", storage, NewFakeTransport()), term)
}

func becomeLeader(node Node, term uint64) *raftNode {
	raftNode := node.(*raftNode)
	raftNode.state = Leader
	raftNode.currentTerm = term
	raftNode.votedFor = raftNode.id
	raftNode.leaderID = raftNode.id
	return raftNode
}

func startReplicationWorker(node *raftNode, peer string) {
	node.wg.Add(1)
	go func() {
		defer node.wg.Done()
		node.replicationWorker(peer, node.replicateNotify[peer])
	}()
}

func appendTerms(t *testing.T, storage *memStorage, terms ...uint64) {
	t.Helper()

	entries := make([]LogEntry, 0, len(terms))
	for i, term := range terms {
		entries = append(entries, LogEntry{
			Index: uint64(i + 1),
			Term:  term,
			Type:  EntryNormal,
			Data:  []byte("x"),
		})
	}
	if err := storage.Append(entries); err != nil {
		t.Fatalf("append entries: %v", err)
	}
}

func appendRepeatedTerm(t *testing.T, storage *memStorage, count int, term uint64) {
	t.Helper()

	entries := make([]LogEntry, 0, count)
	for i := 0; i < count; i++ {
		entries = append(entries, LogEntry{
			Index: uint64(i + 1),
			Term:  term,
			Type:  EntryNormal,
			Data:  []byte("x"),
		})
	}
	if err := storage.Append(entries); err != nil {
		t.Fatalf("append entries: %v", err)
	}
}

type blockingAppendTransport struct {
	mu                sync.Mutex
	started           chan struct{}
	releaseCh         chan struct{}
	appendCount       int
	concurrent        int
	maxConcurrentSeen int
}

func newBlockingAppendTransport() *blockingAppendTransport {
	return &blockingAppendTransport{
		started:   make(chan struct{}, 1),
		releaseCh: make(chan struct{}),
	}
}

func (t *blockingAppendTransport) RequestVote(ctx context.Context, target string, req RequestVoteRequest) (RequestVoteResponse, error) {
	return RequestVoteResponse{}, ErrNodeStopped
}

func (t *blockingAppendTransport) AppendEntries(ctx context.Context, target string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	t.mu.Lock()
	t.appendCount++
	t.concurrent++
	if t.concurrent > t.maxConcurrentSeen {
		t.maxConcurrentSeen = t.concurrent
	}
	t.mu.Unlock()

	select {
	case t.started <- struct{}{}:
	default:
	}

	select {
	case <-t.releaseCh:
	case <-ctx.Done():
		t.mu.Lock()
		t.concurrent--
		t.mu.Unlock()
		return AppendEntriesResponse{}, ctx.Err()
	}

	t.mu.Lock()
	t.concurrent--
	t.mu.Unlock()
	return AppendEntriesResponse{Term: req.Term, Success: true}, nil
}

func (t *blockingAppendTransport) InstallSnapshot(ctx context.Context, target string, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	return InstallSnapshotResponse{}, ErrNodeStopped
}

func (t *blockingAppendTransport) waitStarted(tst *testing.T, timeout time.Duration) {
	tst.Helper()

	select {
	case <-t.started:
	case <-time.After(timeout):
		tst.Fatal("timed out waiting for append request")
	}
}

func (t *blockingAppendTransport) release() {
	close(t.releaseCh)
}

func (t *blockingAppendTransport) stats() (int, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.appendCount, t.maxConcurrentSeen
}

type recordingTransport struct {
	mu       sync.Mutex
	delegate Transport
	batches  []int
}

func (t *recordingTransport) RequestVote(ctx context.Context, target string, req RequestVoteRequest) (RequestVoteResponse, error) {
	return t.delegate.RequestVote(ctx, target, req)
}

func (t *recordingTransport) AppendEntries(ctx context.Context, target string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	t.mu.Lock()
	t.batches = append(t.batches, len(req.Entries))
	t.mu.Unlock()
	return t.delegate.AppendEntries(ctx, target, req)
}

func (t *recordingTransport) InstallSnapshot(ctx context.Context, target string, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	return t.delegate.InstallSnapshot(ctx, target, req)
}

func (t *recordingTransport) entryBatchSizes() []int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]int(nil), t.batches...)
}

type blockingReadTransport struct {
	mu        sync.Mutex
	started   chan struct{}
	releaseCh chan struct{}
	calls     int
}

func newBlockingReadTransport() *blockingReadTransport {
	return &blockingReadTransport{
		started:   make(chan struct{}, 1),
		releaseCh: make(chan struct{}),
	}
}

func (t *blockingReadTransport) RequestVote(ctx context.Context, target string, req RequestVoteRequest) (RequestVoteResponse, error) {
	return RequestVoteResponse{}, ErrNodeStopped
}

func (t *blockingReadTransport) AppendEntries(ctx context.Context, target string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	t.mu.Lock()
	t.calls++
	t.mu.Unlock()

	select {
	case t.started <- struct{}{}:
	default:
	}

	select {
	case <-t.releaseCh:
		return AppendEntriesResponse{
			Term:        req.Term,
			Success:     true,
			ReadContext: req.ReadContext,
		}, nil
	case <-ctx.Done():
		return AppendEntriesResponse{}, ctx.Err()
	}
}

func (t *blockingReadTransport) InstallSnapshot(ctx context.Context, target string, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	return InstallSnapshotResponse{}, ErrNodeStopped
}

func (t *blockingReadTransport) waitStarted(tst *testing.T, timeout time.Duration) {
	tst.Helper()

	select {
	case <-t.started:
	case <-time.After(timeout):
		tst.Fatal("timed out waiting for read confirm request")
	}
}

func (t *blockingReadTransport) release() {
	close(t.releaseCh)
}

func (t *blockingReadTransport) callCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.calls
}

type countingReadTransport struct {
	mu    sync.Mutex
	calls int
}

func (t *countingReadTransport) RequestVote(ctx context.Context, target string, req RequestVoteRequest) (RequestVoteResponse, error) {
	return RequestVoteResponse{}, ErrNodeStopped
}

func (t *countingReadTransport) AppendEntries(ctx context.Context, target string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	t.mu.Lock()
	t.calls++
	t.mu.Unlock()
	return AppendEntriesResponse{
		Term:        req.Term,
		Success:     true,
		ReadContext: req.ReadContext,
	}, nil
}

func (t *countingReadTransport) InstallSnapshot(ctx context.Context, target string, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	return InstallSnapshotResponse{}, ErrNodeStopped
}

func (t *countingReadTransport) callCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.calls
}

var errInjectedHardState = errors.New("injected hard state failure")

type failingHardStateStorage struct {
	hardState HardState
	snapshot  Snapshot
}

type failCommitStorage struct {
	*memStorage
	fail bool
}

type batchCountingStorage struct {
	*memStorage
	mu       sync.Mutex
	maxBatch int
}

func (s *batchCountingStorage) Append(entries []LogEntry) error {
	s.mu.Lock()
	if len(entries) > s.maxBatch {
		s.maxBatch = len(entries)
	}
	s.mu.Unlock()
	return s.memStorage.Append(entries)
}

func (s *batchCountingStorage) maxBatchSize() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxBatch
}

func (s *failCommitStorage) SaveHardState(state HardState) error {
	if s.fail {
		return errInjectedHardState
	}
	return s.memStorage.SaveHardState(state)
}

func (s *failingHardStateStorage) SaveHardState(HardState) error {
	return errInjectedHardState
}

func (s *failingHardStateStorage) LoadHardState() (HardState, error) {
	return s.hardState, nil
}

func (s *failingHardStateStorage) Append(entries []LogEntry) error {
	return nil
}

func (s *failingHardStateStorage) Entries(start, end uint64) ([]LogEntry, error) {
	return nil, ErrEntryNotFound
}

func (s *failingHardStateStorage) LastIndex() (uint64, error) {
	return 0, nil
}

func (s *failingHardStateStorage) Term(index uint64) (uint64, error) {
	if index == 0 {
		return 0, nil
	}
	return 0, ErrEntryNotFound
}

func (s *failingHardStateStorage) TruncateSuffix(index uint64) error {
	return nil
}

func (s *failingHardStateStorage) TruncatePrefix(index uint64) error {
	return nil
}

func (s *failingHardStateStorage) SaveSnapshot(snapshot Snapshot) error {
	s.snapshot = snapshot
	return nil
}

func (s *failingHardStateStorage) LoadSnapshot() (Snapshot, error) {
	return s.snapshot, nil
}

func (s *failingHardStateStorage) ApplySnapshot(snapshot Snapshot) error {
	s.snapshot = snapshot
	return nil
}

func newFailNode(t *testing.T) *raftNode {
	t.Helper()

	node, err := NewNode(Config{
		ID:               "node1",
		Peers:            []string{"node1", "node2", "node3"},
		Storage:          &failingHardStateStorage{},
		Transport:        NewFakeTransport(),
		ElectionTimeout:  time.Second,
		HeartbeatTimeout: 100 * time.Millisecond,
		ApplyBufferSize:  16,
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	return node.(*raftNode)
}

func assertState(t *testing.T, node *raftNode, state StateType, term uint64, votedFor string, leaderID string) {
	t.Helper()

	if node.state != state {
		t.Fatalf("state = %v, want %v", node.state, state)
	}
	if node.currentTerm != term {
		t.Fatalf("term = %d, want %d", node.currentTerm, term)
	}
	if node.votedFor != votedFor {
		t.Fatalf("votedFor = %q, want %q", node.votedFor, votedFor)
	}
	if node.leaderID != leaderID {
		t.Fatalf("leaderID = %q, want %q", node.leaderID, leaderID)
	}
}

func assertFatalStop(t *testing.T, node *raftNode) {
	t.Helper()

	if !node.stopped {
		t.Fatalf("node should be stopped after fatal hard-state failure")
	}
	if !errors.Is(node.nodeErrorLocked(), ErrNodeStopped) {
		t.Fatalf("node error = %v, want %v", node.nodeErrorLocked(), ErrNodeStopped)
	}
	if !errors.Is(node.nodeErrorLocked(), ErrNodeFailed) {
		t.Fatalf("node error = %v, want %v", node.nodeErrorLocked(), ErrNodeFailed)
	}
	if !errors.Is(node.nodeErrorLocked(), errInjectedHardState) {
		t.Fatalf("node error = %v, want %v", node.nodeErrorLocked(), errInjectedHardState)
	}
}

type memStorage struct {
	mu        sync.RWMutex
	hardState HardState
	entries   []LogEntry
	offset    uint64
	snapshot  Snapshot
}

func newMemStorage() *memStorage {
	return &memStorage{
		entries: []LogEntry{
			{Index: 0, Term: 0, Type: EntryNormal},
		},
	}
}

func (s *memStorage) SaveHardState(state HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hardState = state
	return nil
}

func (s *memStorage) LoadHardState() (HardState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hardState, nil
}

func (s *memStorage) Append(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	firstIndex := entries[0].Index
	if firstIndex < s.offset {
		return ErrCompacted
	}
	if firstIndex == s.offset+uint64(len(s.entries)) {
		s.entries = append(s.entries, cloneTestEntries(entries)...)
		return nil
	}
	if firstIndex > s.offset+uint64(len(s.entries)) {
		return ErrStorageConflict
	}

	cut := firstIndex - s.offset
	s.entries = append(s.entries[:cut], cloneTestEntries(entries)...)
	return nil
}

func (s *memStorage) Entries(start, end uint64) ([]LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if start < s.offset {
		return nil, ErrCompacted
	}
	if end < start {
		return nil, ErrEntryNotFound
	}

	first := s.offset
	last := s.offset + uint64(len(s.entries)) - 1
	if start > last+1 || end > last+1 {
		return nil, ErrEntryNotFound
	}

	return cloneTestEntries(s.entries[start-first : end-first]), nil
}

func (s *memStorage) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.offset + uint64(len(s.entries)) - 1, nil
}

func (s *memStorage) Term(index uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if index < s.offset {
		return 0, ErrCompacted
	}

	last := s.offset + uint64(len(s.entries)) - 1
	if index > last {
		return 0, ErrEntryNotFound
	}

	return s.entries[index-s.offset].Term, nil
}

func (s *memStorage) TruncateSuffix(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if index < s.offset {
		return ErrCompacted
	}

	last := s.offset + uint64(len(s.entries)) - 1
	if index >= last {
		return nil
	}

	s.entries = s.entries[:index-s.offset+1]
	return nil
}

func (s *memStorage) TruncatePrefix(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.truncatePrefixLocked(index)
}

func (s *memStorage) SaveSnapshot(snapshot Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if snapshot.Index == 0 {
		return ErrInvalidConfig
	}
	if snapshot.Index < s.offset {
		return ErrCompacted
	}
	last := s.offset + uint64(len(s.entries)) - 1
	if snapshot.Index > last {
		return ErrEntryNotFound
	}
	if s.entries[snapshot.Index-s.offset].Term != snapshot.Term {
		return ErrStorageConflict
	}

	s.snapshot = cloneTestSnapshot(snapshot)
	return s.truncatePrefixLocked(snapshot.Index)
}

func (s *memStorage) LoadSnapshot() (Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneTestSnapshot(s.snapshot), nil
}

func (s *memStorage) ApplySnapshot(snapshot Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if snapshot.Index == 0 {
		return ErrInvalidConfig
	}
	if snapshot.Index < s.snapshot.Index {
		return ErrCompacted
	}

	s.snapshot = cloneTestSnapshot(snapshot)
	s.offset = snapshot.Index
	s.entries = []LogEntry{{Index: snapshot.Index, Term: snapshot.Term, Type: EntryNormal}}
	return nil
}

func (s *memStorage) truncatePrefixLocked(index uint64) error {
	if index <= s.offset {
		return nil
	}

	last := s.offset + uint64(len(s.entries)) - 1
	if index > last {
		return ErrEntryNotFound
	}

	term := s.entries[index-s.offset].Term
	s.entries = append(
		[]LogEntry{{Index: index, Term: term, Type: EntryNormal}},
		cloneTestEntries(s.entries[index-s.offset+1:])...,
	)
	s.offset = index
	return nil
}

func cloneTestEntries(entries []LogEntry) []LogEntry {
	if len(entries) == 0 {
		return nil
	}

	cloned := make([]LogEntry, len(entries))
	for i, entry := range entries {
		cloned[i] = entry
		cloned[i].Data = append([]byte(nil), entry.Data...)
	}
	return cloned
}

func cloneTestSnapshot(snapshot Snapshot) Snapshot {
	return Snapshot{
		Index: snapshot.Index,
		Term:  snapshot.Term,
		Data:  append([]byte(nil), snapshot.Data...),
	}
}
