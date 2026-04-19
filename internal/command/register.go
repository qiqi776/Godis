package command

type Handler func(session Session, args [][]byte) []byte

type Meta struct {
	MinArgs int
	MaxArgs int
	Exec    Handler
}

func (m *Meta) Match(argc int) bool {
	if argc < m.MinArgs {
		return false
	}
	if m.MaxArgs >= 0 && argc > m.MaxArgs {
		return false
	}
	return true
}