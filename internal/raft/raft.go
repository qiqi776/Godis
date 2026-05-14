package raft

import (
	"context"
	"errors"
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
	started  bool
	stopped  bool
	fatalErr error

	currentTerm uint64
	votedFor    string

	commitIndex uint64
	commitTerm  uint64
	lastApplied uint64

	nextIndex    map[string]uint64
	matchIndex   map[string]uint64
	matchScratch []uint64

	storage   Storage
	transport Transport
	applyCh   chan ApplyMsg

	elecTimeout      time.Duration
	heartbeatTimeout time.Duration
	resetElectionCh  chan struct{}
	applyNotifyCh    chan struct{}
	replicateNotify  map[string]chan struct{}
	stopCh           chan struct{}
	stopOnce         sync.Once
	restoreSnapshot  Snapshot

	readConfirmMu sync.Mutex
	readConfirm   *readConfirmCall
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
	commitTerm := uint64(0)
	if commitIndex > 0 {
		if commitIndex == snapshot.Index {
			commitTerm = snapshot.Term
		} else {
			commitTerm, err = config.Storage.Term(commitIndex)
			if err != nil {
				return nil, err
			}
		}
	}

	node := &raftNode{
		id:               config.ID,
		peers:            append([]string(nil), config.Peers...),
		quorum:           config.quorum(),
		state:            Follower,
		currentTerm:      hardState.CurrentTerm,
		votedFor:         hardState.VotedFor,
		commitIndex:      commitIndex,
		commitTerm:       commitTerm,
		lastApplied:      snapshot.Index,
		nextIndex:        make(map[string]uint64),
		matchIndex:       make(map[string]uint64),
		matchScratch:     make([]uint64, 0, len(config.Peers)),
		storage:          config.Storage,
		transport:        config.Transport,
		applyCh:          make(chan ApplyMsg, config.ApplyBufferSize),
		elecTimeout:      config.ElectionTimeout,
		heartbeatTimeout: config.HeartbeatTimeout,
		resetElectionCh:  make(chan struct{}, 1),
		applyNotifyCh:    make(chan struct{}, 1),
		replicateNotify:  make(map[string]chan struct{}),
		stopCh:           make(chan struct{}),
		restoreSnapshot:  snapshot,
	}

	for _, peer := range node.peers {
		node.nextIndex[peer] = lastIndex + 1
		node.matchIndex[peer] = 0
		if peer != node.id {
			node.replicateNotify[peer] = make(chan struct{}, 1)
		}
	}
	node.matchIndex[node.id] = lastIndex
	return node, nil
}

// Raft 节点入口，负责启动所有后台协程并完成初始状态恢复
func (r *raftNode) Start() error {
	r.mu.Lock()
	if r.stopped {
		err := r.nodeErrorLocked()
		r.mu.Unlock()
		return err
	}
	if r.started {
		r.mu.Unlock()
		return nil
	}
	r.started = true
	r.wg.Add(3 + len(r.replicateNotify))
	r.mu.Unlock()

	go func() { defer r.wg.Done(); r.electionLoop() }()
	go func() { defer r.wg.Done(); r.heartbeatLoop() }()
	go func() { defer r.wg.Done(); r.applyLoop() }()
	for peer, notifyCh := range r.replicateNotify {
		peer := peer
		notifyCh := notifyCh
		go func() {
			defer r.wg.Done()
			r.replicationWorker(peer, notifyCh)
		}()
	}
	r.notifyApply()
	return nil
}

// 停止节点
func (r *raftNode) Stop() error {
	r.stopOnce.Do(func() {
		r.mu.Lock()
		if !r.stopped {
			r.stopped = true
		}
		r.mu.Unlock()
		close(r.stopCh)
		r.wg.Wait()
		close(r.applyCh)
	})
	return nil
}

// Leader 接收上层提案，通过 Raft 集群达成共识并最终应用到状态机
func (r *raftNode) Propose(ctx context.Context, data []byte) (uint64, error) {
	r.mu.Lock()
	if r.stopped {
		err := r.nodeErrorLocked()
		r.mu.Unlock()
		return 0, err
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

// 作为 Leader 节点向本地日志追加一条新条目
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

// 上层通知 Raft 做日志压缩
func (r *raftNode) Snapshot(index uint64, data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopped {
		return r.nodeErrorLocked()
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

// 返回当前节点是否是 Leader
func (r *raftNode) IsLeader() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return !r.stopped && r.state == Leader
}

// 返回当前节点的 LeaderID
func (r *raftNode) LeaderID() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leaderID
}

// 上层通过该通道接收已提交的日志条目和快照。
// 通道会在 Stop 完成后关闭。
func (r *raftNode) ApplyCh() <-chan ApplyMsg {
	return r.applyCh
}

// 返回节点错误
func (r *raftNode) nodeErrorLocked() error {
	if r.fatalErr != nil {
		return errors.Join(ErrNodeStopped, ErrNodeFailed, r.fatalErr)
	}
	return ErrNodeStopped
}

// 处理致命错误
func (r *raftNode) failNodeLocked(err error) error {
	if err == nil {
		return nil
	}
	if r.fatalErr == nil {
		r.fatalErr = err
	}
	r.state = Follower
	r.leaderID = ""
	r.votedFor = ""
	r.stopped = true
	go func() {
		_ = r.Stop()
	}()
	return err
}
