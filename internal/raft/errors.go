package raft

import "errors"

var (
	ErrNotLeader       = errors.New("raft: not leader")
	ErrNodeStopped     = errors.New("raft: node stopped")
	ErrInvalidConfig   = errors.New("raft: invalid config")
	ErrEntryNotFound   = errors.New("raft: log entry not found")
	ErrCompacted       = errors.New("raft: log entry compacted")
	ErrStorageConflict = errors.New("raft: storage conflict")
)
