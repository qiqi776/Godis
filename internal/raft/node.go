package raft

import (
	"context"
	"sync"
	"time"
)

type Node interface {
	Start() error
	Stop() error
	Propose(ctx context.Context, data []byte) (uint64, error)
	ReadIndex(ctx context.Context) (uint64, error)
	Snapshot(index uint64, data []byte) error
	IsLeader() bool
	LeaderID() string
	ApplyCh() <-chan ApplyMsg
}

type raftNode struct {
	mu sync.RWMutex
	wg sync.WaitGroup

	id       string
	peers    []string
	quorum   int
	state    StateType
	leaderID string
	stopped  bool

	currentTerm uint64
	votedFor    string

	commitIndex uint64
	lastApplied uint64

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	storage   Storage
	transport Transport
	applyCh   chan ApplyMsg

	elecTimeout      time.Duration
	heartbeatTimeout time.Duration
	resetElectionCh  chan struct{}
	applyNotifyCh    chan struct{}
	stopCh           chan struct{}
	stopOnce         sync.Once
	restoreSnapshot  Snapshot
}

func NewNode(config Config) (Node, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	hardState, err := config.Storage.LoadHardState()
	if err != nil {
		return nil, err
	}
	lastIndex, err := config.Storage.LastIndex()
	if err != nil {
		return nil, err
	}
	snapshot, err := config.Storage.LoadSnapshot()
	if err != nil {
		return nil, err
	}
	if hardState.Commit > lastIndex {
		return nil, ErrInvalidConfig
	}
	commitIndex := hardState.Commit
	if snapshot.Index > commitIndex {
		commitIndex = snapshot.Index
	}

	node := &raftNode{
		id:               config.ID,
		peers:            append([]string(nil), config.Peers...),
		quorum:           config.quorum(),
		state:            Follower,
		currentTerm:      hardState.CurrentTerm,
		votedFor:         hardState.VotedFor,
		commitIndex:      commitIndex,
		lastApplied:      snapshot.Index,
		nextIndex:        make(map[string]uint64),
		matchIndex:       make(map[string]uint64),
		storage:          config.Storage,
		transport:        config.Transport,
		applyCh:          make(chan ApplyMsg, config.ApplyBufferSize),
		elecTimeout:      config.ElectionTimeout,
		heartbeatTimeout: config.HeartbeatTimeout,
		resetElectionCh:  make(chan struct{}, 1),
		applyNotifyCh:    make(chan struct{}, 1),
		stopCh:           make(chan struct{}),
		restoreSnapshot:  snapshot,
	}

	for _, peer := range node.peers {
		node.nextIndex[peer] = lastIndex + 1
		node.matchIndex[peer] = 0
	}
	node.matchIndex[node.id] = lastIndex
	return node, nil
}

func (r *raftNode) Start() error {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return ErrNodeStopped
	}
	r.mu.Unlock()
	if r.restoreSnapshot.Index > 0 {
		r.publishSnapshot(r.restoreSnapshot)
	}

	r.wg.Add(3)
	go func() { defer r.wg.Done(); r.electionLoop() }()
	go func() { defer r.wg.Done(); r.heartbeatLoop() }()
	go func() { defer r.wg.Done(); r.applyLoop() }()
	r.notifyApply()
	return nil
}

func (r *raftNode) Stop() error {
	r.stopOnce.Do(func() {
		r.mu.Lock()
		r.stopped = true
		r.mu.Unlock()
		close(r.stopCh)
		r.wg.Wait()
		close(r.applyCh)
	})
	return nil
}

func (r *raftNode) Propose(ctx context.Context, data []byte) (uint64, error) {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return 0, ErrNodeStopped
	}
	if r.state != Leader {
		r.mu.Unlock()
		return 0, ErrNotLeader
	}

	entry, err := r.appendEntry(EntryNormal, data)
	if err != nil {
		r.mu.Unlock()
		return 0, err
	}
	r.advanceCommitIndex()
	r.mu.Unlock()
	r.replicateAll()
	return entry.Index, nil
}

func (r *raftNode) appendEntry(entryType EntryType, data []byte) (LogEntry, error) {
	lastIndex, err := r.storage.LastIndex()
	if err != nil {
		return LogEntry{}, err
	}
	entry := LogEntry{
		Index: lastIndex + 1,
		Term:  r.currentTerm,
		Type:  entryType,
		Data:  append([]byte(nil), data...),
	}
	if err := r.storage.Append([]LogEntry{entry}); err != nil {
		return LogEntry{}, err
	}

	r.matchIndex[r.id] = entry.Index
	r.nextIndex[r.id] = entry.Index + 1
	return entry, nil
}

func (r *raftNode) Snapshot(index uint64, data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopped {
		return ErrNodeStopped
	}
	if index == 0 || index > r.lastApplied {
		return ErrEntryNotFound
	}

	cur, err := r.storage.LoadSnapshot()
	if err != nil {
		return err
	}
	if index <= cur.Index {
		return nil
	}
	term, err := r.storage.Term(index)
	if err != nil {
		return err
	}

	return r.storage.SaveSnapshot(Snapshot{
		Index: index,
		Term:  term,
		Data:  append([]byte(nil), data...),
	})
}

func (r *raftNode) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.stopped && r.state == Leader
}

func (r *raftNode) LeaderID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaderID
}

func (r *raftNode) ApplyCh() <-chan ApplyMsg {
	return r.applyCh
}
