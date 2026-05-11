package faults

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"mini-kv/internal/raft"
)

var ErrDropped = errors.New("perfkit faults: message dropped")

type Controller struct {
	mu     sync.RWMutex
	random *rand.Rand

	links  map[linkKey]linkRule
	paused map[string]bool
}

type linkKey struct {
	from string
	to   string
}

type linkRule struct {
	delay    time.Duration
	dropRate float64
	blocked  bool
}

func NewController(seed int64) *Controller {
	return &Controller{
		random: rand.New(rand.NewSource(seed)),
		links:  make(map[linkKey]linkRule),
		paused: make(map[string]bool),
	}
}

func (c *Controller) Wrap(localID string, transport raft.Transport) raft.Transport {
	return &Transport{
		localID:    localID,
		transport:  transport,
		controller: c,
	}
}

func (c *Controller) Delay(from, to string, delay time.Duration) {
	c.updateLink(from, to, func(rule linkRule) linkRule {
		rule.delay = delay
		return rule
	})
}

func (c *Controller) DropRate(from, to string, rate float64) {
	if rate < 0 {
		rate = 0
	}
	if rate > 1 {
		rate = 1
	}
	c.updateLink(from, to, func(rule linkRule) linkRule {
		rule.dropRate = rate
		return rule
	})
}

func (c *Controller) Block(from, to string) {
	c.updateLink(from, to, func(rule linkRule) linkRule {
		rule.blocked = true
		return rule
	})
}

func (c *Controller) Unblock(from, to string) {
	c.updateLink(from, to, func(rule linkRule) linkRule {
		rule.blocked = false
		return rule
	})
}

func (c *Controller) Isolate(id string, peers []string) {
	for _, peer := range peers {
		if peer == id {
			continue
		}
		c.Block(id, peer)
		c.Block(peer, id)
	}
}

func (c *Controller) Heal(id string, peers []string) {
	for _, peer := range peers {
		if peer == id {
			continue
		}
		c.Unblock(id, peer)
		c.Unblock(peer, id)
	}
}

func (c *Controller) Pause(id string) {
	c.mu.Lock()
	c.paused[id] = true
	c.mu.Unlock()
}

func (c *Controller) Resume(id string) {
	c.mu.Lock()
	delete(c.paused, id)
	c.mu.Unlock()
}

func (c *Controller) ResetAll() {
	c.mu.Lock()
	c.links = make(map[linkKey]linkRule)
	c.paused = make(map[string]bool)
	c.mu.Unlock()
}

func (c *Controller) updateLink(from, to string, update func(linkRule) linkRule) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := linkKey{from: from, to: to}
	c.links[key] = update(c.links[key])
}

func (c *Controller) beforeSend(ctx context.Context, from, to string) error {
	c.mu.Lock()
	rule := c.links[linkKey{from: from, to: to}]
	paused := c.paused[from] || c.paused[to]
	drop := false
	if rule.dropRate > 0 {
		drop = c.random.Float64() < rule.dropRate
	}
	c.mu.Unlock()

	if paused || rule.blocked {
		return waitBlocked(ctx)
	}
	if drop {
		return ErrDropped
	}
	if rule.delay <= 0 {
		return nil
	}
	timer := time.NewTimer(rule.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitBlocked(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}
