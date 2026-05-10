package raft

import "errors"

type StateType uint8

const (
	Follower StateType = iota + 1
	Candidate
	Leader
)

var (
	ErrNotLeader         = errors.New("raft: not leader")
	ErrNodeStopped       = errors.New("raft: node stopped")
	ErrInvalidConfig     = errors.New("raft: invalid config")
	ErrEntryNotFound     = errors.New("raft: log entry not found")
	ErrCompacted         = errors.New("raft: log entry compacted")
	ErrStorageConflict   = errors.New("raft: storage conflict")
	ErrReadIndexNotReady = errors.New("raft: read index not ready")
)

func (s StateType) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

type EntryType uint8

const (
	EntryNormal EntryType = iota + 1
	EntryNoop
)

type LogEntry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

type ApplyMsg struct {
	Index        uint64
	Term         uint64
	Type         EntryType
	Data         []byte
	Snapshot     bool
	SnapshotData []byte
}

type Snapshot struct {
	Index uint64
	Term  uint64
	Data  []byte
}

type HardState struct {
	CurrentTerm uint64
	VotedFor    string
	Commit      uint64
}

type RequestVoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

type InstallSnapshotRequest struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type InstallSnapshotResponse struct {
	Term uint64
}
