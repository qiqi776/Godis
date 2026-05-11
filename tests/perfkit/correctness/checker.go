package correctness

import (
	"bytes"
	"fmt"
	"time"

	"github.com/anishathalye/porcupine"
)

type CheckReport struct {
	Result     porcupine.CheckResult
	Checked    int
	SkippedErr int
}

type modelInput struct {
	Type  OperationType
	Key   string
	Value string
}

type modelOutput struct {
	Value string
	Found bool
}

type modelState struct {
	Value string
	Found bool
}

func Check(history []Operation, timeout time.Duration) CheckReport {
	operations, skipped := toPorcupineOperations(history)
	return CheckReport{
		Result:     porcupine.CheckOperationsTimeout(Model(), operations, timeout),
		Checked:    len(operations),
		SkippedErr: skipped,
	}
}

func Model() porcupine.Model {
	return porcupine.Model{
		Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
			byKey := make(map[string][]porcupine.Operation)
			keys := make([]string, 0)
			for _, operation := range history {
				input := operation.Input.(modelInput)
				if _, ok := byKey[input.Key]; !ok {
					keys = append(keys, input.Key)
				}
				byKey[input.Key] = append(byKey[input.Key], operation)
			}
			partitions := make([][]porcupine.Operation, 0, len(keys))
			for _, key := range keys {
				partitions = append(partitions, byKey[key])
			}
			return partitions
		},
		Init: func() interface{} {
			return modelState{}
		},
		Step: func(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
			current := state.(modelState)
			in := input.(modelInput)
			out := output.(modelOutput)
			switch in.Type {
			case OpSet:
				return true, modelState{Value: in.Value, Found: true}
			case OpGet:
				if current.Found != out.Found {
					return false, state
				}
				if current.Found && current.Value != out.Value {
					return false, state
				}
				return true, state
			case OpDelete:
				return true, modelState{}
			default:
				return false, state
			}
		},
		Equal: func(state1 interface{}, state2 interface{}) bool {
			left := state1.(modelState)
			right := state2.(modelState)
			return left == right
		},
		DescribeOperation: func(input interface{}, output interface{}) string {
			in := input.(modelInput)
			out := output.(modelOutput)
			switch in.Type {
			case OpSet:
				return fmt.Sprintf("set(%s, %s)", in.Key, in.Value)
			case OpGet:
				if !out.Found {
					return fmt.Sprintf("get(%s) -> missing", in.Key)
				}
				return fmt.Sprintf("get(%s) -> %s", in.Key, out.Value)
			case OpDelete:
				return fmt.Sprintf("delete(%s)", in.Key)
			default:
				return string(in.Type)
			}
		},
		DescribeState: func(state interface{}) string {
			current := state.(modelState)
			if !current.Found {
				return "missing"
			}
			return current.Value
		},
	}
}

func toPorcupineOperations(history []Operation) ([]porcupine.Operation, int) {
	if len(history) == 0 {
		return nil, 0
	}
	origin := history[0].Call
	for _, operation := range history[1:] {
		if operation.Call.Before(origin) {
			origin = operation.Call
		}
	}

	operations := make([]porcupine.Operation, 0, len(history))
	skipped := 0
	for _, operation := range history {
		if operation.Err != "" {
			skipped++
			continue
		}
		operations = append(operations, porcupine.Operation{
			ClientId: operation.ClientID,
			Input: modelInput{
				Type:  operation.Type,
				Key:   operation.Key,
				Value: string(operation.Value),
			},
			Call: nanosSince(origin, operation.Call),
			Output: modelOutput{
				Value: string(operation.Output.Value),
				Found: operation.Output.Found,
			},
			Return: nanosSince(origin, operation.Return),
		})
	}
	return operations, skipped
}

func nanosSince(origin time.Time, current time.Time) int64 {
	if current.IsZero() {
		return 0
	}
	return current.Sub(origin).Nanoseconds()
}

func EqualBytes(left []byte, right []byte) bool {
	return bytes.Equal(left, right)
}
