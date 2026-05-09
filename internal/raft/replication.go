package raft

import (
	"context"
	"sort"
)

// 向所有节点发送心跳/日志复制
func (r *raftNode) replicateAll() {
	r.mu.RLock()
	if r.stopped || r.state != Leader {
		r.mu.RUnlock()
		return
	}

	peers := append([]string(nil), r.peers...)
	r.mu.RUnlock()

	for _, peer := range peers {
		if peer == r.id {
			continue
		}
		go r.replicate(peer)
	}
}

func (r *raftNode) replicate(peer string) {
	for {
		snapshotReq, term, ok := r.buildInstallSnapshotRequest(peer)
		if ok {
			ctx, cancel := context.WithTimeout(context.Background(), r.heartbeatTimeout)
			resp, err := r.transport.InstallSnapshot(ctx, peer, snapshotReq)
			cancel()
			if err != nil {
				return
			}
			if resp.Term > term {
				r.stepDown(resp.Term, "")
				return
			}
			r.handleInstallSnapshotSuccess(peer, snapshotReq)
			return
		}
		req, term, ok := r.buildAppendEntriesRequest(peer)
		if !ok {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), r.heartbeatTimeout)
		resp, err := r.transport.AppendEntries(ctx, peer, req)
		cancel()
		if err != nil {
			return
		}
		if resp.Term > term {
			r.stepDown(resp.Term, "")
			return
		}
		if resp.Success {
			r.handleAppendEntriesSuccess(peer, req)
			return
		}
		if !r.decreaseNextIndex(peer, term) {
			return
		}
	}
}

func (r *raftNode) buildInstallSnapshotRequest(peer string) (InstallSnapshotRequest, uint64, bool) {
	r.mu.RLock()
	if r.stopped || r.state != Leader {
		r.mu.RUnlock()
		return InstallSnapshotRequest{}, 0, false
	}

	term := r.currentTerm
	nextIndex := r.nextIndex[peer]
	snapshot, err := r.storage.LoadSnapshot()
	r.mu.RUnlock()
	if err != nil || snapshot.Index == 0 || nextIndex > snapshot.Index {
		return InstallSnapshotRequest{}, 0, false
	}

	return InstallSnapshotRequest{
		Term:              term,
		LeaderID:          r.id,
		LastIncludedIndex: snapshot.Index,
		LastIncludedTerm:  snapshot.Term,
		Data:              append([]byte(nil), snapshot.Data...),
	}, term, true
}

func (r *raftNode) buildAppendEntriesRequest(peer string) (AppendEntriesRequest, uint64, bool) {
	r.mu.RLock()
	if r.stopped || r.state != Leader {
		r.mu.RUnlock()
		return AppendEntriesRequest{}, 0, false
	}

	term := r.currentTerm
	leaderID := r.id
	leaderCommit := r.commitIndex
	nextIndex := r.nextIndex[peer]
	if nextIndex == 0 {
		nextIndex = 1
	}
	prevLogIndex := nextIndex - 1
	r.mu.RUnlock()

	prevLogTerm, err := r.storage.Term(prevLogIndex)
	if err != nil {
		return AppendEntriesRequest{}, 0, false
	}

	lastIndex, err := r.storage.LastIndex()
	if err != nil {
		return AppendEntriesRequest{}, 0, false
	}

	var entries []LogEntry
	if nextIndex <= lastIndex {
		entries, err = r.storage.Entries(nextIndex, lastIndex+1)
		if err != nil {
			return AppendEntriesRequest{}, 0, false
		}
	}
	return AppendEntriesRequest{
		Term:         term,
		LeaderID:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}, term, true
}

func (r *raftNode) handleAppendEntriesSuccess(peer string, req AppendEntriesRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopped || r.state != Leader || req.Term != r.currentTerm {
		return
	}

	matchIndex := req.PrevLogIndex + uint64(len(req.Entries))
	r.matchIndex[peer] = matchIndex
	r.nextIndex[peer] = matchIndex + 1

	r.advanceCommitIndex()
}

func (r *raftNode) decreaseNextIndex(peer string, term uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopped || r.state != Leader || r.currentTerm != term {
		return false
	}

	minNextIndex := uint64(1)
	if snapshot, err := r.storage.LoadSnapshot(); err == nil && snapshot.Index > 0 {
		minNextIndex = snapshot.Index
	}
	if r.nextIndex[peer] <= minNextIndex {
		return false
	}

	r.nextIndex[peer]--
	return true
}

func (r *raftNode) handleInstallSnapshotSuccess(peer string, req InstallSnapshotRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopped || r.state != Leader || req.Term != r.currentTerm {
		return
	}

	r.matchIndex[peer] = req.LastIncludedIndex
	r.nextIndex[peer] = req.LastIncludedIndex + 1
}

// 检查是否有新日志可以在当前 Term 提交，更新 commitIndex 并将已提交日志应用到状态机
func (r *raftNode) advanceCommitIndex() {
	indexes := make([]uint64, 0, len(r.peers))
	for _, peer := range r.peers {
		indexes = append(indexes, r.matchIndex[peer])
	}
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i] < indexes[j]
	})

	majorityIndex := indexes[len(indexes)-r.quorum]
	if majorityIndex <= r.commitIndex {
		return
	}

	term, err := r.storage.Term(majorityIndex)
	if err != nil {
		return
	}

	if term != r.currentTerm {
		return
	}

	r.commitIndex = majorityIndex
	if err := r.persistState(); err != nil {
		return
	}
	r.notifyApply()
}
