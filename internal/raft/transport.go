package raft

import "context"

type Transport interface {
	RequestVote(ctx context.Context, target string, req RequestVoteRequest) (RequestVoteResponse, error)
	AppendEntries(ctx context.Context, target string, req AppendEntriesRequest) (AppendEntriesResponse, error)
}
