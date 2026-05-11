package raft

import (
	"context"
	"time"
)

type readConfirmResult struct {
	term uint64
	ok   bool
}

func (r *raftNode) ReadIndex(ctx context.Context) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		_, term, ready, err := r.readState()
		if err != nil {
			return 0, err
		}
		if ready {
			if err := r.confirm(ctx, term); err == nil {
				r.mu.RLock()
				defer r.mu.RUnlock()
				if r.stopped {
					return 0, r.nodeErrorLocked()
				}
				if r.state != Leader || r.currentTerm != term {
					return 0, ErrNotLeader
				}
				return r.commitIndex, nil
			}
		}

		r.replicateAll()

		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			stop(timer)
			return 0, ctx.Err()
		case <-r.stopCh:
			stop(timer)
			r.mu.RLock()
			err := r.nodeErrorLocked()
			r.mu.RUnlock()
			return 0, err
		}
	}
}

func (r *raftNode) readState() (uint64, uint64, bool, error) {
	r.mu.RLock()
	if r.stopped {
		err := r.nodeErrorLocked()
		r.mu.RUnlock()
		return 0, 0, false, err
	}
	if r.state != Leader {
		r.mu.RUnlock()
		return 0, 0, false, ErrNotLeader
	}
	index := r.commitIndex
	term := r.currentTerm
	r.mu.RUnlock()

	if index == 0 {
		return index, term, false, nil
	}
	commitTerm, err := r.storage.Term(index)
	if err != nil {
		return 0, 0, false, err
	}
	return index, term, commitTerm == term, nil
}

func (r *raftNode) confirm(ctx context.Context, term uint64) error {
	r.mu.RLock()
	if r.stopped {
		err := r.nodeErrorLocked()
		r.mu.RUnlock()
		return err
	}
	if r.state != Leader || r.currentTerm != term {
		r.mu.RUnlock()
		return ErrNotLeader
	}

	peers := append([]string(nil), r.peers...)
	quorum := r.quorum
	timeout := r.heartbeatTimeout
	r.mu.RUnlock()

	if quorum <= 1 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	results := make(chan readConfirmResult, len(peers)-1)
	for _, peer := range peers {
		if peer == r.id {
			continue
		}

		go func(target string) {
			req, ok := r.buildReadIndexRequest(target, term)
			if !ok {
				results <- readConfirmResult{}
				return
			}
			resp, err := r.transport.AppendEntries(ctx, target, req)
			if err != nil {
				results <- readConfirmResult{}
				return
			}
			results <- readConfirmResult{
				term: resp.Term,
				ok:   resp.Success,
			}
		}(peer)
	}

	acks := 1
	remain := len(peers) - 1
	for remain > 0 {
		select {
		case result := <-results:
			remain--
			if result.term > term {
				if err := r.stepDown(result.term, ""); err != nil {
					return err
				}
				return ErrNotLeader
			}
			if result.ok {
				acks++
				if acks >= quorum {
					return nil
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stopCh:
			r.mu.RLock()
			err := r.nodeErrorLocked()
			r.mu.RUnlock()
			return err
		}
	}

	return ErrReadIndexNotReady
}

func (r *raftNode) buildReadIndexRequest(peer string, term uint64) (AppendEntriesRequest, bool) {
	r.mu.RLock()
	if r.stopped || r.state != Leader || r.currentTerm != term {
		r.mu.RUnlock()
		return AppendEntriesRequest{}, false
	}

	nextIndex := r.nextIndex[peer]
	if nextIndex == 0 {
		nextIndex = 1
	}
	req := AppendEntriesRequest{
		Term:         r.currentTerm,
		LeaderID:     r.id,
		PrevLogIndex: nextIndex - 1,
		LeaderCommit: r.commitIndex,
	}
	r.mu.RUnlock()

	prevTerm, err := r.storage.Term(req.PrevLogIndex)
	if err != nil {
		return AppendEntriesRequest{}, false
	}
	req.PrevLogTerm = prevTerm
	return req, true
}
