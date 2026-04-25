package engine

type SnapshotCommandProvider interface {
	SnapshotCommands() [][][]byte
}

func (e *Engine) SnapshotCommands() [][][]byte {
	return e.AOFRewriteCommands()
}