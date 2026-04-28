package raft

type Storage interface {
	SaveHardState(state HardState) error
	LoadHardState() (HardState, error)
	Append(entries []LogEntry) error
	Entries(start, end uint64) ([]LogEntry, error)
	LastIndex() (uint64, error)
	Term(index uint64) (uint64, error)
	TruncateSuffix(index uint64) error
	TruncatePrefix(index uint64) error
}
