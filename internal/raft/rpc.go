package raft

import "context"

func (r *raftNode) HandleRequestVote(ctx context.Context, req RequestVoteRequest) (RequestVoteResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return RequestVoteResponse{}, ErrNodeStopped
	}
	if req.Term < r.currentTerm {
		return RequestVoteResponse{
			Term:        r.currentTerm,
			VoteGranted: false,
		}, nil
	}
	if req.Term > r.currentTerm {
		prev := r.snapshotState()
		r.state = Follower
		r.currentTerm = req.Term
		r.votedFor = ""
		r.leaderID = ""
		if err := r.persist(prev); err != nil {
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
		return AppendEntriesResponse{}, ErrNodeStopped
	}
	if req.Term < r.currentTerm {
		return AppendEntriesResponse{
			Term:    r.currentTerm,
			Success: false,
		}, nil
	}
	if req.Term > r.currentTerm {
		prev := r.snapshotState()
		r.state = Follower
		r.leaderID = ""
		r.currentTerm = req.Term
		r.votedFor = ""
		if err := r.persist(prev); err != nil {
			return AppendEntriesResponse{}, err
		}
	}

	r.state = Follower
	r.leaderID = req.LeaderID
	r.resetElectionTimer()

	prevTerm, err := r.storage.Term(req.PrevLogIndex)
	if err != nil || prevTerm != req.PrevLogTerm {
		return AppendEntriesResponse{
			Term:    r.currentTerm,
			Success: false,
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
		r.commitIndex = min(req.LeaderCommit, lastIndex)
		if err := r.persistState(); err != nil {
			return AppendEntriesResponse{}, err
		}
		r.notifyApply()
	}

	return AppendEntriesResponse{
		Term:    r.currentTerm,
		Success: true,
	}, nil
}

func (r *raftNode) HandleInstallSnapshot(ctx context.Context, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return InstallSnapshotResponse{}, ErrNodeStopped
	}
	if req.Term < r.currentTerm {
		term := r.currentTerm
		r.mu.Unlock()
		return InstallSnapshotResponse{Term: term}, nil
	}
	if req.Term > r.currentTerm {
		prev := r.snapshotState()
		r.state = Follower
		r.leaderID = ""
		r.currentTerm = req.Term
		r.votedFor = ""
		if err := r.persist(prev); err != nil {
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
		if err := r.storage.ApplySnapshot(snapshot); err != nil {
			r.mu.Unlock()
			return InstallSnapshotResponse{}, err
		}
		if r.commitIndex < req.LastIncludedIndex {
			r.commitIndex = req.LastIncludedIndex
		}
		r.lastApplied = req.LastIncludedIndex
		if err := r.persistState(); err != nil {
			r.mu.Unlock()
			return InstallSnapshotResponse{}, err
		}
	}
	term := r.currentTerm
	r.mu.Unlock()

	if shouldApply {
		r.publishSnapshot(snapshot)
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
