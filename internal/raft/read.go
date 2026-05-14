package raft

import (
	"context"
	"sync/atomic"
	"time"
)

type readConfirmResult struct {
	term        uint64
	ok          bool
	readContext uint64
}

type readConfirmCall struct {
	term uint64
	done chan struct{}
	err  error
}

var readContextSeq atomic.Uint64
var readConfirmBatchWindow = 100 * time.Microsecond

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
	commitTerm := r.commitTerm
	r.mu.RUnlock()

	if index == 0 {
		return index, term, false, nil
	}
	return index, term, commitTerm == term, nil
}

func (r *raftNode) confirm(ctx context.Context, term uint64) error {
	if ctx == nil {
		ctx = context.Background()
	}

	peers, quorum, timeout, err := r.readConfirmState(term)
	if err != nil {
		return err
	}
	if quorum <= 1 {
		return nil
	}

	return r.sharedReadConfirm(ctx, term, peers, quorum, timeout)
}

func (r *raftNode) readConfirmState(term uint64) ([]string, int, time.Duration, error) {
	r.mu.RLock()
	if r.stopped {
		err := r.nodeErrorLocked()
		r.mu.RUnlock()
		return nil, 0, 0, err
	}
	if r.state != Leader || r.currentTerm != term {
		r.mu.RUnlock()
		return nil, 0, 0, ErrNotLeader
	}

	peers := r.peers
	quorum := r.quorum
	timeout := r.heartbeatTimeout
	r.mu.RUnlock()
	return peers, quorum, timeout, nil
}

func (r *raftNode) sharedReadConfirm(ctx context.Context, term uint64, peers []string, quorum int, timeout time.Duration) error {
	r.readConfirmMu.Lock()
	if call := r.readConfirm; call != nil && call.term == term {
		select {
		case <-call.done:
		default:
			r.readConfirmMu.Unlock()
			return waitReadConfirm(ctx, call)
		}
	}

	call := &readConfirmCall{
		term: term,
		done: make(chan struct{}),
	}
	r.readConfirm = call
	r.readConfirmMu.Unlock()

	go r.runReadConfirm(call, term, peers, quorum, timeout)
	return waitReadConfirm(ctx, call)
}

func waitReadConfirm(ctx context.Context, call *readConfirmCall) error {
	select {
	case <-call.done:
		return call.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *raftNode) runReadConfirm(call *readConfirmCall, term uint64, peers []string, quorum int, timeout time.Duration) {
	if err := r.waitReadConfirmBatchWindow(); err != nil {
		call.err = err
	} else {
		call.err = r.confirmQuorum(term, peers, quorum, timeout)
	}
	close(call.done)

	r.readConfirmMu.Lock()
	if r.readConfirm == call {
		r.readConfirm = nil
	}
	r.readConfirmMu.Unlock()
}

func (r *raftNode) waitReadConfirmBatchWindow() error {
	if readConfirmBatchWindow <= 0 {
		return nil
	}

	timer := time.NewTimer(readConfirmBatchWindow)
	select {
	case <-timer.C:
		return nil
	case <-r.stopCh:
		stop(timer)
		r.mu.RLock()
		err := r.nodeErrorLocked()
		r.mu.RUnlock()
		return err
	}
}

func (r *raftNode) confirmQuorum(term uint64, peers []string, quorum int, timeout time.Duration) error {
	readContext := readContextSeq.Add(1)
	if readContext == 0 {
		readContext = readContextSeq.Add(1)
	}

	req, ok := r.buildReadIndexRequest(term, readContext)
	if !ok {
		return ErrNotLeader
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	results := make(chan readConfirmResult, len(peers)-1)
	for _, peer := range peers {
		if peer == r.id {
			continue
		}

		go func(target string) {
			resp, err := r.transport.AppendEntries(ctx, target, req)
			if err != nil {
				results <- readConfirmResult{}
				return
			}
			results <- readConfirmResult{
				term:        resp.Term,
				ok:          resp.Success,
				readContext: resp.ReadContext,
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
			if result.ok && result.readContext == readContext {
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

func (r *raftNode) buildReadIndexRequest(term uint64, readContext uint64) (AppendEntriesRequest, bool) {
	r.mu.RLock()
	if r.stopped || r.state != Leader || r.currentTerm != term {
		r.mu.RUnlock()
		return AppendEntriesRequest{}, false
	}
	req := AppendEntriesRequest{
		Term:         r.currentTerm,
		LeaderID:     r.id,
		LeaderCommit: r.commitIndex,
		ReadContext:  readContext,
	}
	r.mu.RUnlock()
	return req, true
}
