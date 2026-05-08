package raft

import (
	"context"
	"math/rand"
	"time"
)

type voteResult struct {
	term    uint64
	granted bool
}

func (r *raftNode) electionLoop() {
	for {
		timeout := r.randomTimeout()
		timer := time.NewTimer(timeout)
		select {
		case <-timer.C:
			if !r.isLeaderorStop() {
				r.startElection()
			}
		case <-r.resetElectionCh:
			if !timer.Stop() {
				<-timer.C
			}
		case <-r.stopCh:
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}
}

func (r *raftNode) heartbeatLoop() {
	ticker := time.NewTicker(r.heartbeatTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.replicateAll()
		case <-r.stopCh:
			return
		}
	}
}

func (r *raftNode) startElection() {
	r.mu.Lock()
	if r.stopped || r.state == Leader {
		r.mu.Unlock()
		return
	}

	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.id
	r.leaderID = ""
	term := r.currentTerm
	peers := append([]string(nil), r.peers...)
	quorum := r.quorum
	if err := r.persistState(); err != nil {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	lastLogIndex, err := r.storage.LastIndex()
	if err != nil {
		return
	}
	lastLogTerm, err := r.storage.Term(lastLogIndex)
	if err != nil {
		return
	}

	votes := 1
	if votes >= quorum {
		r.beLeader(term)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.elecTimeout)
	defer cancel()

	results := make(chan voteResult, len(peers)-1)
	for _, peer := range peers {
		if peer == r.id {
			continue
		}
		go func(tar string) {
			resp, err := r.transport.RequestVote(ctx, tar, RequestVoteRequest{
				Term:         term,
				CandidateID:  r.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil {
				results <- voteResult{}
				return
			}
			results <- voteResult{
				term:    resp.Term,
				granted: resp.VoteGranted,
			}
		}(peer)
	}

	remain := len(peers) - 1
	for remain > 0 {
		select {
		case result := <-results:
			remain--
			if result.term > term {
				r.stepDown(result.term, "")
				return
			}
			if !result.granted {
				continue
			}
			votes++
			if votes >= quorum {
				r.beLeader(term)
				return
			}
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		}
	}

}

func (r *raftNode) beLeader(term uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped || r.state != Candidate || r.currentTerm != term {
		return
	}

	r.state = Leader
	r.leaderID = r.id

	noop, err := r.appendEntry(EntryNoop, nil)
	if err != nil {
		r.state = Follower
		r.leaderID = ""
		return
	}
	for _, peer := range r.peers {
		r.nextIndex[peer] = noop.Index + 1
		r.matchIndex[peer] = 0
	}
	r.matchIndex[r.id] = noop.Index
	r.nextIndex[r.id] = noop.Index + 1

	r.advanceCommitIndex()
	go r.replicateAll()
}

func (r *raftNode) stepDown(term uint64, leaderID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if term <= r.currentTerm {
		return
	}

	termChanged := term > r.currentTerm
	r.state = Follower
	r.leaderID = leaderID
	r.currentTerm = term
	if termChanged {
		r.votedFor = ""
		_ = r.persistState()
	}

	r.resetElectionTimer()
}

func (r *raftNode) persistState() error {
	return r.storage.SaveHardState(HardState{
		CurrentTerm: r.currentTerm,
		VotedFor:    r.votedFor,
		Commit:      r.commitIndex,
	})
}

func (r *raftNode) resetElectionTimer() {
	select {
	case r.resetElectionCh <- struct{}{}:
	default:
	}
}

func (r *raftNode) notifyApply() {
	select {
	case r.applyNotifyCh <- struct{}{}:
	default:
	}
}

func (r *raftNode) randomTimeout() time.Duration {
	t := time.Duration(rand.Int63n(int64(r.elecTimeout)))
	return r.elecTimeout + t
}

func (r *raftNode) isLeaderorStop() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopped || r.state == Leader
}

func (r *raftNode) isLogUpToDate(lastLogIndex, lastLogTerm uint64) bool {
	localLastIndex, err := r.storage.LastIndex()
	if err != nil {
		return false
	}
	localLastTerm, err := r.storage.Term(localLastIndex)
	if err != nil {
		return false
	}
	if lastLogTerm != localLastTerm {
		return lastLogTerm > localLastTerm
	}
	return lastLogIndex >= localLastIndex
}

func stop(timer *time.Timer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}
