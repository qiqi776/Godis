package correctness

import (
	"sync"
	"time"
)

type OperationType string

const (
	OpGet    OperationType = "get"
	OpSet    OperationType = "set"
	OpDelete OperationType = "delete"
)

type Operation struct {
	ClientID int
	Type     OperationType
	Key      string
	Value    []byte
	Call     time.Time
	Return   time.Time
	Output   Output
	Err      string
}

type Output struct {
	Value []byte
	Found bool
}

type History struct {
	mu         sync.Mutex
	operations []Operation
}

func (h *History) Record(operation Operation) {
	h.mu.Lock()
	h.operations = append(h.operations, cloneOperation(operation))
	h.mu.Unlock()
}

func (h *History) Operations() []Operation {
	h.mu.Lock()
	defer h.mu.Unlock()

	operations := make([]Operation, len(h.operations))
	for i, operation := range h.operations {
		operations[i] = cloneOperation(operation)
	}
	return operations
}

func cloneOperation(operation Operation) Operation {
	operation.Value = append([]byte(nil), operation.Value...)
	operation.Output.Value = append([]byte(nil), operation.Output.Value...)
	return operation
}
