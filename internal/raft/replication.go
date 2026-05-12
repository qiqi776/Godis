package raft

import (
	"context"
	"sort"
)

const replicationBatchSize = 64

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
		r.notifyReplication(peer)
	}
}

func (r *raftNode) notifyReplication(peer string) {
	notifyCh := r.replicateNotify[peer]
	if notifyCh == nil {
		return
	}
	select {
	case notifyCh <- struct{}{}:
	default:
	}
}

func (r *raftNode) replicationWorker(peer string, notifyCh <-chan struct{}) {
	for {
		select {
		case <-notifyCh:
			r.replicatePeer(peer)
		case <-r.stopCh:
			return
		}
	}
}

// 单个 follower 的复制 worker,每次被唤醒后串行发送，直到追平 backlog 或遇到错误
func (r *raftNode) replicatePeer(peer string) {
	for r.replicateStep(peer) {
	}
}

func (r *raftNode) replicateStep(peer string) bool {
	snapshotReq, term, ok := r.buildInstallSnapshotRequest(peer)
	if ok {
		ctx, cancel := context.WithTimeout(context.Background(), r.heartbeatTimeout)
		resp, err := r.transport.InstallSnapshot(ctx, peer, snapshotReq)
		cancel()
		if err != nil {
			return false
		}
		if resp.Term > term {
			_ = r.stepDown(resp.Term, "")
			return false
		}
		r.handleInstallSnapshot(peer, snapshotReq)
		return r.peerNeedsReplication(peer)
	}

	req, term, ok := r.buildAppendEntriesRequest(peer)
	if !ok {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.heartbeatTimeout)
	resp, err := r.transport.AppendEntries(ctx, peer, req)
	cancel()
	if err != nil {
		return false
	}
	if resp.Term > term {
		_ = r.stepDown(resp.Term, "")
		return false
	}
	if resp.Success {
		r.handleAppendEntries(peer, req)
		return len(req.Entries) > 0 && r.peerNeedsReplication(peer)
	}
	return r.decreaseNextIndex(peer, term, resp)
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
		endIndex := min(lastIndex+1, nextIndex+replicationBatchSize)
		entries, err = r.storage.Entries(nextIndex, endIndex)
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

func (r *raftNode) handleAppendEntries(peer string, req AppendEntriesRequest) {
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

// 失败回退索引
func (r *raftNode) decreaseNextIndex(peer string, term uint64, resp AppendEntriesResponse) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopped || r.state != Leader || r.currentTerm != term {
		return false
	}

	minNextIndex := uint64(1)
	if snapshot, err := r.storage.LoadSnapshot(); err == nil && snapshot.Index > 0 {
		minNextIndex = snapshot.Index
	}
	currentNextIndex := r.nextIndex[peer]
	if currentNextIndex <= minNextIndex {
		return false
	}

	nextIndex := currentNextIndex - 1
	switch {
	case resp.ConflictTerm != 0:
		if lastIndex, ok := r.findLastIndexOfTermLocked(resp.ConflictTerm); ok {
			nextIndex = lastIndex + 1
		} else if resp.ConflictIndex > 0 {
			nextIndex = resp.ConflictIndex
		}
	case resp.ConflictIndex > 0:
		nextIndex = resp.ConflictIndex
	}

	if nextIndex < minNextIndex {
		nextIndex = minNextIndex
	}
	if nextIndex >= currentNextIndex {
		nextIndex = currentNextIndex - 1
		if nextIndex < minNextIndex {
			nextIndex = minNextIndex
		}
	}
	if nextIndex == currentNextIndex {
		return false
	}

	r.nextIndex[peer] = nextIndex
	return true
}

func (r *raftNode) handleInstallSnapshot(peer string, req InstallSnapshotRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopped || r.state != Leader || req.Term != r.currentTerm {
		return
	}

	r.matchIndex[peer] = req.LastIncludedIndex
	r.nextIndex[peer] = req.LastIncludedIndex + 1
}

func (r *raftNode) findLastIndexOfTermLocked(term uint64) (uint64, bool) {
	lastIndex, err := r.storage.LastIndex()
	if err != nil {
		return 0, false
	}
	for index := lastIndex; ; index-- {
		localTerm, err := r.storage.Term(index)
		if err != nil {
			return 0, false
		}
		if localTerm == term {
			return index, true
		}
		if localTerm < term || index == 0 {
			return 0, false
		}
	}
}

func (r *raftNode) peerNeedsReplication(peer string) bool {
	r.mu.RLock()
	if r.stopped || r.state != Leader {
		r.mu.RUnlock()
		return false
	}
	nextIndex := r.nextIndex[peer]
	r.mu.RUnlock()

	if nextIndex == 0 {
		nextIndex = 1
	}

	snapshot, err := r.storage.LoadSnapshot()
	if err == nil && snapshot.Index > 0 && nextIndex <= snapshot.Index {
		return true
	}

	lastIndex, err := r.storage.LastIndex()
	if err != nil {
		return false
	}
	return nextIndex <= lastIndex
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

	if err := r.updateCommitIndexLocked(majorityIndex); err != nil {
		return
	}
	r.notifyApply()
}
