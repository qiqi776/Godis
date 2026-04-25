package command

import "godis/internal/resp"

func (e *Executor) execBGRewriteAOF(_ Session, _ [][]byte) []byte {
	if e.rewriter == nil {
		return resp.Error("ERR AOF is not enabled")
	}
	if err := e.rewriter.Rewrite(e.engine.SnapshotCommands); err != nil {
		return resp.Error("ERR AOF rewrite failed")
	}
	return resp.SimpleString("OK")
}