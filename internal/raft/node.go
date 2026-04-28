package raft

import (
	"context"
	"sync"
)

type Node interface {
	Start() error
	Stop() error
	Propose(ctx context.Context, data []byte) (uint64, error)
	IsLeader() bool
	LeaderID() string
	ApplyCh() <-chan ApplyMsg
}

type raftNode struct {
	mu sync.RWMutex

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
}

func NewNode(config Config) (Node, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	hardState, err := config.Storage.LoadHardState()
	if err != nil {
		return nil, err
	}
	node := &raftNode{
		id:          config.ID,
		peers:       append([]string(nil), config.Peers...),
		quorum:      config.quorum(),
		state:       Follower,
		currentTerm: hardState.CurrentTerm,
		votedFor:    hardState.VotedFor,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		storage:     config.Storage,
		transport:   config.Transport,
		applyCh:     make(chan ApplyMsg, config.ApplyBufferSize),
	}
	lastIndex, err := config.Storage.LastIndex()
	if err != nil {
		return nil, err
	}

	for _, peer := range node.peers {
		node.nextIndex[peer] = lastIndex + 1
		node.matchIndex[peer] = 0
	}
	return node, nil
}

func (n *raftNode) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.stopped {
		return ErrNodeStopped
	}
	return nil
}

func (n *raftNode) Stop() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.stopped {
		return nil
	}

	n.stopped = true
	close(n.applyCh)
	return nil
}

func (n *raftNode) Propose(ctx context.Context, data []byte) (uint64, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.stopped {
		return 0, ErrNodeStopped
	}
	if n.state != Leader {
		return 0, ErrNotLeader
	}

	return 0, nil
}

func (n *raftNode) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return !n.stopped && n.state == Leader
}

func (n *raftNode) LeaderID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.leaderID
}

func (n *raftNode) ApplyCh() <-chan ApplyMsg {
	return n.applyCh
}
