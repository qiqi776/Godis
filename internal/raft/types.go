package raft

type StateType uint8

const (
	Follower StateType = iota + 1
	Candidate
	Leader
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
)

type LogEntry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

type ApplyMsg struct {
	Index uint64
	Term  uint64
	Data  []byte
}

type HardState struct {
	CurrentTerm uint64
	VotedFor    string
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
