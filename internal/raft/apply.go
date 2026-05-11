package raft

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

		r.mu.Lock()
		if r.stopped || r.lastApplied >= r.commitIndex {
			r.mu.Unlock()
			return
		}

		nextIndex := r.lastApplied + 1
		r.mu.Unlock()

		// 读取日志条目并处理快照压缩
		entries, err := r.storage.Entries(nextIndex, nextIndex+1)
		if err == ErrCompacted {
			snapshot, snapshotErr := r.storage.LoadSnapshot()
			if snapshotErr != nil || snapshot.Index == 0 {
				return
			}
			r.mu.Lock()
			if r.lastApplied < snapshot.Index {
				r.lastApplied = snapshot.Index
			}
			r.mu.Unlock()
			continue
		}
		if err != nil || len(entries) == 0 {
			return
		}

		entry := entries[0]
		msg := ApplyMsg{
			Index: entry.Index,
			Term:  entry.Term,
			Type:  entry.Type,
			Data:  append([]byte(nil), entry.Data...),
		}

		select {
		case r.applyCh <- msg:
			r.mu.Lock()
			if r.lastApplied < entry.Index {
				r.lastApplied = entry.Index
			}
			r.mu.Unlock()
		case <-r.stopCh:
			return
		}
	}
}

// 通过 apply loop 串行推送待恢复快照，保证 applyCh 只有一个生产者。
func (r *raftNode) applyRestoreSnapshot() bool {
	r.mu.RLock()
	if r.stopped || r.restoreSnapshot.Index == 0 {
		r.mu.RUnlock()
		return false
	}
	snapshot := Snapshot{
		Index: r.restoreSnapshot.Index,
		Term:  r.restoreSnapshot.Term,
		Data:  append([]byte(nil), r.restoreSnapshot.Data...),
	}
	r.mu.RUnlock()

	msg := ApplyMsg{
		Index:        snapshot.Index,
		Term:         snapshot.Term,
		Snapshot:     true,
		SnapshotData: append([]byte(nil), snapshot.Data...),
	}
	select {
	case r.applyCh <- msg:
		r.mu.Lock()
		if r.restoreSnapshot.Index == snapshot.Index && r.restoreSnapshot.Term == snapshot.Term {
			r.restoreSnapshot = Snapshot{}
		}
		if r.lastApplied < snapshot.Index {
			r.lastApplied = snapshot.Index
		}
		r.mu.Unlock()
		return true
	case <-r.stopCh:
		return false
	}
}
