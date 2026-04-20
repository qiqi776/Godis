package command

import "godis/internal/resp"

func (e *Executor) execMulti(session Session, _ [][]byte) []byte {
	if !session.StartMulti() {
		return resp.Error("ERR MULTI calls can not be nested")
	}
	return resp.SimpleString("OK")
}

func (e *Executor) execExec(session Session, _ [][]byte) []byte {
	if !session.InMulti() {
		return resp.Error("ERR EXEC without MULTI")
	}

	queued := session.Queued()
	session.ClearMulti()

	replies := make([][]byte, 0, len(queued))
	for _, tokens := range queued {
		replies = append(replies, e.Execute(session, tokens))
	}
	return resp.ArrayReplies(replies)
}

func (e *Executor) execDiscard(session Session, _ [][]byte) []byte {
	if !session.InMulti() {
		return resp.Error("ERR DISCARD without MULTI")
	}

	session.ClearMulti()
	return resp.SimpleString("OK")
}
