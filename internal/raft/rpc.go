package raft

import (
	"context"
	"errors"
)

func (r *raftNode) HandleRequestVote(ctx context.Context, req RequestVoteRequest) (RequestVoteResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return RequestVoteResponse{}, r.nodeErrorLocked()
	}
	// 收到更高 Term
	if req.Term < r.currentTerm {
		return RequestVoteResponse{
			Term:        r.currentTerm,
			VoteGranted: false,
		}, nil
	}
	if req.Term > r.currentTerm {
		if err := r.stepDownLocked(req.Term, ""); err != nil {
			return RequestVoteResponse{}, err
		}
	}

	canVote := r.votedFor == "" || r.votedFor == req.CandidateID
	logOK := r.isLogUpToDate(req.LastLogIndex, req.LastLogTerm)

	if canVote && logOK {
		prev := r.snapshotState()
		r.state = Follower
		r.votedFor = req.CandidateID
		r.leaderID = ""
		if err := r.persist(prev); err != nil {
			return RequestVoteResponse{}, err
		}
		r.resetElectionTimer()

		return RequestVoteResponse{
			Term:        r.currentTerm,
			VoteGranted: true,
		}, nil
	}
	return RequestVoteResponse{
		Term:        r.currentTerm,
		VoteGranted: false,
	}, nil
}

func (r *raftNode) HandleAppendEntries(ctx context.Context, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return AppendEntriesResponse{}, r.nodeErrorLocked()
	}
	if req.Term < r.currentTerm {
		return AppendEntriesResponse{
			Term:    r.currentTerm,
			Success: false,
		}, nil
	}
	if req.Term > r.currentTerm {
		if err := r.stepDownLocked(req.Term, ""); err != nil {
			return AppendEntriesResponse{}, err
		}
	}

	r.state = Follower
	r.leaderID = req.LeaderID
	r.resetElectionTimer()

	if req.ReadContext != 0 {
		return AppendEntriesResponse{
			Term:        r.currentTerm,
			Success:     true,
			ReadContext: req.ReadContext,
		}, nil
	}

	prevTerm, err := r.storage.Term(req.PrevLogIndex)
	if err != nil {
		conflictIndex, conflictTerm, conflictErr := r.appendConflictHintLocked(req.PrevLogIndex)
		if conflictErr != nil {
			return AppendEntriesResponse{}, conflictErr
		}
		return AppendEntriesResponse{
			Term:          r.currentTerm,
			Success:       false,
			ReadContext:   req.ReadContext,
			ConflictIndex: conflictIndex,
			ConflictTerm:  conflictTerm,
		}, nil
	}
	if prevTerm != req.PrevLogTerm {
		conflictIndex, conflictErr := r.firstIndexOfTermLocked(prevTerm, req.PrevLogIndex)
		if conflictErr != nil {
			return AppendEntriesResponse{}, conflictErr
		}
		return AppendEntriesResponse{
			Term:          r.currentTerm,
			Success:       false,
			ReadContext:   req.ReadContext,
			ConflictIndex: conflictIndex,
			ConflictTerm:  prevTerm,
		}, nil
	}

	if len(req.Entries) > 0 {
		if err := r.appendEntries(req.Entries); err != nil {
			return AppendEntriesResponse{}, err
		}
	}

	lastIndex, err := r.storage.LastIndex()
	if err != nil {
		return AppendEntriesResponse{}, err
	}

	if req.LeaderCommit > r.commitIndex {
		nextCommit := min(req.LeaderCommit, lastIndex)
		if err := r.updateCommitIndexLocked(nextCommit); err != nil {
			return AppendEntriesResponse{}, err
		}
		r.notifyApply()
	}

	return AppendEntriesResponse{
		Term:        r.currentTerm,
		Success:     true,
		ReadContext: req.ReadContext,
	}, nil
}

func (r *raftNode) HandleInstallSnapshot(ctx context.Context, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return InstallSnapshotResponse{}, r.nodeErrorLocked()
	}
	if req.Term < r.currentTerm {
		term := r.currentTerm
		r.mu.Unlock()
		return InstallSnapshotResponse{Term: term}, nil
	}
	if req.Term > r.currentTerm {
		if err := r.stepDownLocked(req.Term, ""); err != nil {
			r.mu.Unlock()
			return InstallSnapshotResponse{}, err
		}
	}
	r.state = Follower
	r.leaderID = req.LeaderID
	r.resetElectionTimer()
	snapshot := Snapshot{
		Index: req.LastIncludedIndex,
		Term:  req.LastIncludedTerm,
		Data:  append([]byte(nil), req.Data...),
	}
	shouldApply := req.LastIncludedIndex > r.lastApplied
	if shouldApply {
		nextCommit := r.commitIndex
		if req.LastIncludedIndex > nextCommit {
			nextCommit = req.LastIncludedIndex
		}
		if err := r.storage.ApplySnapshot(snapshot); err != nil {
			r.mu.Unlock()
			return InstallSnapshotResponse{}, err
		}
		if err := r.updateCommitIndexLocked(nextCommit); err != nil {
			r.mu.Unlock()
			return InstallSnapshotResponse{}, err
		}
		r.lastApplied = req.LastIncludedIndex
		r.restoreSnapshot = snapshot
	}
	term := r.currentTerm
	r.mu.Unlock()

	if shouldApply {
		r.notifyApply()
	}
	return InstallSnapshotResponse{Term: term}, nil
}

func (r *raftNode) appendEntries(entries []LogEntry) error {
	for _, entry := range entries {
		localTerm, err := r.storage.Term(entry.Index)
		if err == nil {
			if localTerm != entry.Term {
				if err := r.storage.TruncateSuffix(entry.Index - 1); err != nil {
					return err
				}
				return r.storage.Append(entriesFrom(entries, entry.Index))
			}
			continue
		}

		if err == ErrEntryNotFound {
			return r.storage.Append(entriesFrom(entries, entry.Index))
		}

		return err
	}

	return nil
}

func entriesFrom(entries []LogEntry, index uint64) []LogEntry {
	for i, entry := range entries {
		if entry.Index == index {
			return entries[i:]
		}
	}
	return nil
}

func (r *raftNode) appendConflictHintLocked(prevLogIndex uint64) (uint64, uint64, error) {
	lastIndex, err := r.storage.LastIndex()
	if err != nil {
		return 0, 0, err
	}
	if prevLogIndex > lastIndex {
		return lastIndex + 1, 0, nil
	}

	snapshot, err := r.storage.LoadSnapshot()
	if err != nil {
		return 0, 0, err
	}
	if snapshot.Index > 0 && prevLogIndex < snapshot.Index {
		return snapshot.Index + 1, 0, nil
	}

	term, err := r.storage.Term(prevLogIndex)
	if err == nil {
		conflictIndex, findErr := r.firstIndexOfTermLocked(term, prevLogIndex)
		if findErr != nil {
			return 0, 0, findErr
		}
		return conflictIndex, term, nil
	}
	if errors.Is(err, ErrCompacted) {
		if snapshot.Index > 0 {
			return snapshot.Index + 1, 0, nil
		}
		return 1, 0, nil
	}
	if errors.Is(err, ErrEntryNotFound) {
		return lastIndex + 1, 0, nil
	}
	return 0, 0, err
}

func (r *raftNode) firstIndexOfTermLocked(term uint64, maxIndex uint64) (uint64, error) {
	firstIndex := maxIndex
	for firstIndex > 0 {
		prevIndex := firstIndex - 1
		prevTerm, err := r.storage.Term(prevIndex)
		if errors.Is(err, ErrCompacted) || errors.Is(err, ErrEntryNotFound) {
			return firstIndex, nil
		}
		if err != nil {
			return 0, err
		}
		if prevTerm != term {
			return firstIndex, nil
		}
		firstIndex = prevIndex
	}
	return firstIndex, nil
}
