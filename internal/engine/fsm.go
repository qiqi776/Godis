package engine

import (
	"fmt"
	"time"
)

const defaultFSMDBIndex = 0

func (e *Engine) Apply(command KVCommand) ApplyResult {
	return e.ApplyToDB(defaultFSMDBIndex, command)
}

func (e *Engine) ApplyToDB(dbIndex int, command KVCommand) ApplyResult {
	db := e.DB(dbIndex)
	if db == nil {
		return ApplyResult{Error: fmt.Sprintf("db index out of range: %d", dbIndex)}
	}
	return db.Apply(command)
}

func (db *DB) Apply(command KVCommand) ApplyResult {
	switch command.Type {
	case CommandPut:
		return db.applyPut(command)
	case CommandDelete:
		return db.applyDelete(command)
	case CommandExpire:
		return db.applyExpire(command)
	case CommandPersist:
		return db.applyPersist(command)
	default:
		return ApplyResult{Error: fmt.Sprintf("unknown command type: %d", command.Type)}
	}
}

func (db *DB) applyPut(command KVCommand) ApplyResult {
	if command.ExpireAt < 0 {
		return ApplyResult{Error: "expire timestamp must not be negative"}
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	db.setValue(command.Key, KindString, copyBytes(command.Value))
	if command.ExpireAt > 0 {
		db.expireAt[command.Key] = time.UnixMilli(command.ExpireAt)
	} else {
		delete(db.expireAt, command.Key)
	}

	return ApplyResult{Found: true}
}

func (db *DB) applyDelete(command KVCommand) ApplyResult {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.isExpired(command.Key) {
		db.deleteKey(command.Key)
		return ApplyResult{Found: false}
	}
	if _, ok := db.data[command.Key]; !ok {
		return ApplyResult{Found: false}
	}

	db.deleteKey(command.Key)
	return ApplyResult{Found: true}
}

func (db *DB) applyExpire(command KVCommand) ApplyResult {
	if command.ExpireAt <= 0 {
		return ApplyResult{Error: "expire timestamp must be positive"}
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.isExpired(command.Key) {
		db.deleteKey(command.Key)
		return ApplyResult{Found: false}
	}
	if _, ok := db.data[command.Key]; !ok {
		return ApplyResult{Found: false}
	}

	db.expireAt[command.Key] = time.UnixMilli(command.ExpireAt)
	db.touchKey(command.Key)
	return ApplyResult{Found: true}
}

func (db *DB) applyPersist(command KVCommand) ApplyResult {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.isExpired(command.Key) {
		db.deleteKey(command.Key)
		return ApplyResult{Found: false}
	}
	if _, ok := db.data[command.Key]; !ok {
		return ApplyResult{Found: false}
	}
	if _, ok := db.expireAt[command.Key]; !ok {
		return ApplyResult{Found: false}
	}

	delete(db.expireAt, command.Key)
	db.touchKey(command.Key)
	return ApplyResult{Found: true}
}
