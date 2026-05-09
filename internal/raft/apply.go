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

// 推送快照给应用层
func (r *raftNode) publishSnapshot(snapshot Snapshot) {
	msg := ApplyMsg{
		Index:        snapshot.Index,
		Term:         snapshot.Term,
		Snapshot:     true,
		SnapshotData: append([]byte(nil), snapshot.Data...),
	}
	select {
	case r.applyCh <- msg:
	case <-r.stopCh:
	}
}
