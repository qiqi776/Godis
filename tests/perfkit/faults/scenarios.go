package faults

import (
	"context"
	"time"
)

type Scenario interface {
	Apply(ctx context.Context, controller *Controller) error
}

type ScenarioFunc func(ctx context.Context, controller *Controller) error

func (f ScenarioFunc) Apply(ctx context.Context, controller *Controller) error {
	return f(ctx, controller)
}

func TemporaryLeaderPartition(leaderID string, peers []string, duration time.Duration) Scenario {
	return ScenarioFunc(func(ctx context.Context, controller *Controller) error {
		controller.Isolate(leaderID, peers)
		defer controller.Heal(leaderID, peers)
		return wait(ctx, duration)
	})
}

func FollowerLag(followerID string, peers []string, delay time.Duration, duration time.Duration) Scenario {
	return ScenarioFunc(func(ctx context.Context, controller *Controller) error {
		for _, peer := range peers {
			if peer == followerID {
				continue
			}
			controller.Delay(peer, followerID, delay)
		}
		defer controller.ResetAll()
		return wait(ctx, duration)
	})
}

func MinoritySlowKill(ids []string, duration time.Duration) Scenario {
	return ScenarioFunc(func(ctx context.Context, controller *Controller) error {
		for _, id := range ids {
			controller.Pause(id)
		}
		defer func() {
			for _, id := range ids {
				controller.Resume(id)
			}
		}()
		return wait(ctx, duration)
	})
}

func NetworkFlap(peers []string, dropRate float64, delay time.Duration, interval time.Duration, rounds int) Scenario {
	return ScenarioFunc(func(ctx context.Context, controller *Controller) error {
		defer controller.ResetAll()
		for i := 0; i < rounds; i++ {
			for fromIndex, from := range peers {
				for toIndex, to := range peers {
					if fromIndex == toIndex {
						continue
					}
					controller.DropRate(from, to, dropRate)
					controller.Delay(from, to, delay)
				}
			}
			if err := wait(ctx, interval); err != nil {
				return err
			}
			controller.ResetAll()
			if err := wait(ctx, interval); err != nil {
				return err
			}
		}
		return nil
	})
}

func wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
