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

type raftStateSnapshot struct {
	state       StateType
	leaderID    string
	currentTerm uint64
	votedFor    string
}

// 启动选举
func (r *raftNode) electionLoop() {
	for {
		timeout := r.randomTimeout()
		timer := time.NewTimer(timeout)
		select {
		case <-timer.C:
			if !r.isLeaderorStop() {
				r.Election()
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

// 发送心跳
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

// 用于发起领导者选举
// 当前节点处于 Follower 或者 Candidate 且选举计时器超时则调用此方法
// 将节点提升为 Candidate 增加当前任期，为自己投票，并向其他所有节点并行请求投票
// 一旦获得法定人数的选举，节点立即成为 Leader；若收到更高任期则退回 Follower
func (r *raftNode) Election() {
	r.mu.Lock()
	if r.stopped || r.state == Leader {
		r.mu.Unlock()
		return
	}

	// 先启动当前快照，确保持久化失败能够回滚
	prev := r.snapshotState()

	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.id
	r.leaderID = ""
	term := r.currentTerm
	peers := append([]string(nil), r.peers...)
	quorum := r.quorum

	// 原子持久化，写入磁盘
	if err := r.persist(prev); err != nil {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	// 读取存储中的日志最新索引和任期，在投票请求中发给其他节点
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

	// 并行向其他节点发送投票请求
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

	// 等待应答
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

// 节点成为领导者
func (r *raftNode) beLeader(term uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped || r.state != Candidate || r.currentTerm != term {
		return
	}

	r.state = Leader
	r.leaderID = r.id

	// 新 Leader 上任后必须立即追加一条空日志
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

// 节点收到比当前大的 Term，执行降级
func (r *raftNode) stepDown(term uint64, leaderID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if term <= r.currentTerm {
		return
	}

	prev := r.snapshotState()
	r.state = Follower
	r.leaderID = leaderID
	r.currentTerm = term
	r.votedFor = ""
	if err := r.persist(prev); err != nil {
		return
	}
	r.resetElectionTimer()
}

// 状态快照与恢复
func (r *raftNode) snapshotState() raftStateSnapshot {
	return raftStateSnapshot{
		state:       r.state,
		leaderID:    r.leaderID,
		currentTerm: r.currentTerm,
		votedFor:    r.votedFor,
	}
}

func (r *raftNode) restoreState(snapshot raftStateSnapshot) {
	r.state = snapshot.state
	r.leaderID = snapshot.leaderID
	r.currentTerm = snapshot.currentTerm
	r.votedFor = snapshot.votedFor
}

// 带快照回滚的持久化
func (r *raftNode) persist(snapshot raftStateSnapshot) error {
	if err := r.persistState(); err != nil {
		r.restoreState(snapshot)
		return err
	}
	return nil
}

// 持久化
func (r *raftNode) persistState() error {
	return r.storage.SaveHardState(HardState{
		CurrentTerm: r.currentTerm,
		VotedFor:    r.votedFor,
		Commit:      r.commitIndex,
	})
}

// 重置选举计时器
func (r *raftNode) resetElectionTimer() {
	select {
	case r.resetElectionCh <- struct{}{}:
	default:
	}
}

// 异步通知
func (r *raftNode) notifyApply() {
	select {
	case r.applyNotifyCh <- struct{}{}:
	default:
	}
}

// 随机超时时间，防止多个节点同时发起选举导致票数分裂
func (r *raftNode) randomTimeout() time.Duration {
	t := time.Duration(rand.Int63n(int64(r.elecTimeout)))
	return r.elecTimeout + t
}

// 判断节点是否是 Leader 或者是否停止
func (r *raftNode) isLeaderorStop() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopped || r.state == Leader
}

// 比较新旧日志
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

// 定时器安全停止
func stop(timer *time.Timer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}
