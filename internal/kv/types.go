package kv

type CommandType uint8

const (
	CommandPut CommandType = iota + 1
	CommandDelete
)

type Command struct {
	Type      CommandType
	Key       string
	Value     []byte
	ClientID  string
	RequestID uint64
}

type ApplyResult struct {
	Value []byte
	Found bool
	Error string
}

// StateMachine is the replicated KV state machine used in the current
// single-group Raft baseline.
type StateMachine interface {
	Apply(command Command) ApplyResult
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

type FSM = StateMachine

type Store interface {
	StateMachine
	Reader() Reader
}

type Reader interface {
	Get(key string) ([]byte, bool, error)
}
