package command

import "godis/internal/resp"

func (e *Executor) execWatch(session Session, args [][]byte) []byte {
    if session.InMulti() {
        return resp.Error("ERR WATCH inside MULTI is not allowed")
    }

    dbIndex := session.GetDBIndex()
    db := e.engine.DB(dbIndex)
    if db == nil {
        return resp.Error("ERR DB index is out of range")
    }

    for _, arg := range args {
        key := string(arg)
        session.Watch(dbIndex, key, db.Revision(key))
    }
    return resp.SimpleString("OK")
}

func (e *Executor) watchDirty(session Session) bool {
    for dbIndex, keys := range session.Watched() {
        db := e.engine.DB(dbIndex)
        if db == nil {
            return true
        }
        for key, rev := range keys {
            if db.Revision(key) != rev {
                return true
            }
        }
    }
    return false
}