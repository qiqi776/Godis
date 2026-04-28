package raft

import "context"

func (n *raftNode) HandleRequestVote(ctx context.Context, req RequestVoteRequest) (RequestVoteResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.stopped {
		return RequestVoteResponse{}, ErrNodeStopped
	}

	return RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
	}, nil
}

func (n *raftNode) HandleAppendEntries(ctx context.Context, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.stopped {
		return AppendEntriesResponse{}, ErrNodeStopped
	}

	return AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: false,
	}, nil
}
