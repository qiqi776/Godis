package engine

type MutationEvent struct {
	DBIndex int
	Command [][]byte
}

type MutationHook interface {
	ApplyMutation(event MutationEvent) error
}
