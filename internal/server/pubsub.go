package server

import (
	"godis/internal/resp"
	"net"
	"sync"
)

type PubSub struct {
	mu sync.RWMutex
	subs map[string]map[net.Conn]struct{}
}

func NewPubSub() *PubSub {
	return &PubSub{
		subs: make(map[string]map[net.Conn]struct{}),
	}
}

func (ps *PubSub) Subscribe(conn net.Conn, channels ...string) int {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	total := 0
	for _, channel := range channels {
		if ps.subs[channel] == nil {
			ps.subs[channel] = make(map[net.Conn]struct{})
		}
		if _, ok := ps.subs[channel][conn]; ok {
			continue
		}
		ps.subs[channel][conn] = struct{}{}
		total++
	}
	return total
}

func (ps *PubSub) Unsubscribe(conn net.Conn, channels ...string) int {
    ps.mu.Lock()
    defer ps.mu.Unlock()

    total := 0
    for _, channel := range channels {
        subs := ps.subs[channel]
        if subs == nil {
            continue
        }
        if _, ok := subs[conn]; ok {
            delete(subs, conn)
            total++
        }
        if len(subs) == 0 {
            delete(ps.subs, channel)
        }
    }
    return total
}

func (ps *PubSub) UnsubscribeAll(conn net.Conn) []string {
    ps.mu.Lock()
    defer ps.mu.Unlock()

    out := make([]string, 0)
    for channel, subs := range ps.subs {
        if _, ok := subs[conn]; !ok {
            continue
        }
        delete(subs, conn)
        out = append(out, channel)
        if len(subs) == 0 {
            delete(ps.subs, channel)
        }
    }
    return out
}

func (ps *PubSub) Publish(channel string, msg []byte) int64 {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	subs := ps.subs[channel]
	if len(subs) == 0 {
		return 0
	}

	reply := pubsubMeg(channel, msg)
	var delivered int64
	for conn := range subs {
		if _, err := conn.Write(reply); err == nil {
			delivered++
		}
	}
	return delivered
}

func pubsubSubscribeReply(channel string, count int) []byte {
    return resp.ArrayReplies([][]byte{
        resp.BulkString([]byte("subscribe")),
        resp.BulkString([]byte(channel)),
        resp.Integer(int64(count)),
    })
}

func pubsubUnsubscribeReply(channel string, count int) []byte {
    return resp.ArrayReplies([][]byte{
        resp.BulkString([]byte("unsubscribe")),
        resp.BulkString([]byte(channel)),
        resp.Integer(int64(count)),
    })
}

func pubsubMeg(channel string, msg []byte) []byte {
	return resp.ArrayReplies([][]byte{
		resp.BulkString([]byte("message")),
		resp.BulkString([]byte(channel)),
		resp.BulkString(msg),
	})
}