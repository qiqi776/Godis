package observability

import (
	"context"
	"time"

	"mini-kv/internal/raft"
)

const DefaultRaftSampleInterval = 200 * time.Millisecond

func StartRaftSampler(ctx context.Context, registry *Registry, nodeID string, node raft.Node, storage raft.Storage, interval time.Duration) {
	if registry == nil || nodeID == "" || node == nil || storage == nil {
		return
	}
	if interval <= 0 {
		interval = DefaultRaftSampleInterval
	}

	go func() {
		lastState := ""
		sample := func() {
			state := "follower"
			if node.IsLeader() {
				state = "leader"
			}
			if state == "leader" && lastState != "leader" {
				registry.IncLeaderChange(nodeID)
			}
			lastState = state

			registry.SetRaftState(nodeID, state)
			registry.SetRaftLeader(nodeID, node.LeaderID())
			if hardState, err := storage.LoadHardState(); err == nil {
				registry.SetCommitIndex(nodeID, hardState.Commit)
			}
		}

		sample()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sample()
			}
		}
	}()
}
