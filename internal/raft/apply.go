package raft

const applyBatchSize = 64

// 日志应用
func (r *raftNode) applyLoop() {
	for {
		select {
		case <-r.applyNotifyCh:
			r.applyCommitEntries()
		case <-r.stopCh:
			return
		}
	}
}

// 应用已提交日志
func (r *raftNode) applyCommitEntries() {
	for {
		if r.applyRestoreSnapshot() {
			continue
		}

		entries, ok := r.nextApplyEntries()
		if !ok {
			return
		}

		restart := false
		for _, entry := range entries {
			msg, ok := r.makeApplyMsg(entry)
			if !ok {
				break
			}

			select {
			case r.applyCh <- msg:
				r.mu.Lock()
				if r.lastApplied+1 == entry.Index {
					r.lastApplied = entry.Index
				}
				r.mu.Unlock()
			case <-r.applyNotifyCh:
				restart = true
			case <-r.stopCh:
				return
			}
			if restart {
				break
			}
		}
		if restart {
			continue
		}
	}
}

func (r *raftNode) nextApplyEntries() ([]LogEntry, bool) {
	r.mu.RLock()
	if r.stopped || r.lastApplied >= r.commitIndex {
		r.mu.RUnlock()
		return nil, false
	}
	nextIndex := r.lastApplied + 1
	endIndex := min(r.commitIndex+1, nextIndex+applyBatchSize)
	r.mu.RUnlock()

	entries, err := r.storage.Entries(nextIndex, endIndex)
	if err == ErrCompacted {
		snapshot, snapshotErr := r.storage.LoadSnapshot()
		if snapshotErr != nil || snapshot.Index == 0 {
			return nil, false
		}
		r.mu.Lock()
		if r.lastApplied < snapshot.Index {
			r.lastApplied = snapshot.Index
		}
		r.mu.Unlock()
		return nil, true
	}
	if err != nil || len(entries) == 0 {
		return nil, false
	}
	return entries, true
}

func (r *raftNode) makeApplyMsg(entry LogEntry) (ApplyMsg, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.stopped || r.restoreSnapshot.Index > 0 || entry.Index != r.lastApplied+1 || entry.Index > r.commitIndex {
		return ApplyMsg{}, false
	}
	return ApplyMsg{
		Index: entry.Index,
		Term:  entry.Term,
		Type:  entry.Type,
		Data:  append([]byte(nil), entry.Data...),
	}, true
}

// 通过 apply loop 串行推送待恢复快照，保证 applyCh 只有一个生产者。
func (r *raftNode) applyRestoreSnapshot() bool {
	r.mu.RLock()
	if r.stopped || r.restoreSnapshot.Index == 0 {
		r.mu.RUnlock()
		return false
	}
	snapshotIndex := r.restoreSnapshot.Index
	snapshotTerm := r.restoreSnapshot.Term
	snapshotData := append([]byte(nil), r.restoreSnapshot.Data...)
	r.mu.RUnlock()

	msg := ApplyMsg{
		Index:        snapshotIndex,
		Term:         snapshotTerm,
		Snapshot:     true,
		SnapshotData: snapshotData,
	}
	select {
	case r.applyCh <- msg:
		r.mu.Lock()
		if r.restoreSnapshot.Index == snapshotIndex && r.restoreSnapshot.Term == snapshotTerm {
			r.restoreSnapshot = Snapshot{}
		}
		if r.lastApplied < snapshotIndex {
			r.lastApplied = snapshotIndex
		}
		r.mu.Unlock()
		return true
	case <-r.applyNotifyCh:
		return true
	case <-r.stopCh:
		return false
	}
}
