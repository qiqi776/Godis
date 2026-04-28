package engine

type CommandType uint8

const (
	CommandPut CommandType = iota + 1
	CommandDelete
	CommandExpire
	CommandPersist
)

type KVCommand struct {
	Type 	  CommandType
	Key  	  string
	Value 	  []byte
	ExpireAt  int64
	ClientID  string
	RequestID uint64
}

type ApplyResult struct {
	Value []byte
	Found bool
	Error string
}

type FSM interface {
	Apply(command KVCommand) ApplyResult
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}