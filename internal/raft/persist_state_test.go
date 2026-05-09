package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

var errInjectedHardState = errors.New("injected hard state failure")

type failingHardStateStorage struct {
	hardState HardState
	snapshot  Snapshot
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

	assertState(t, node, Leader, 4, node.id, node.id)
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

	assertState(t, node, Candidate, 4, node.id, "")
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

	assertState(t, node, Candidate, 4, node.id, "")
}

func TestStepDownRollback(t *testing.T) {
	node := newFailNode(t)
	node.state = Leader
	node.currentTerm = 4
	node.votedFor = node.id
	node.leaderID = node.id

	node.stepDown(5, "node2")

	assertState(t, node, Leader, 4, node.id, node.id)
}
