package obs

import (
	"time"

	"mini-kv/internal/raft"
)

type NodeSample struct {
	NodeID   string
	At       time.Time
	IsLeader bool
	LeaderID string
}

func SampleNode(nodeID string, node raft.Node) NodeSample {
	if node == nil {
		return NodeSample{NodeID: nodeID, At: time.Now()}
	}
	return NodeSample{
		NodeID:   nodeID,
		At:       time.Now(),
		IsLeader: node.IsLeader(),
		LeaderID: node.LeaderID(),
	}
}
