package bench

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestShouldIgnoreCancellationAtMeasurementDeadline(t *testing.T) {
	ctx := deadlineOnlyContext{deadline: time.Now().Add(-time.Millisecond)}
	err := status.Error(codes.DeadlineExceeded, "context deadline exceeded")

	if !shouldIgnoreCancellation(ctx, err) {
		t.Fatal("deadline error at measurement deadline should be ignored")
	}
}

func TestShouldNotIgnoreCancellationBeforeMeasurementDeadline(t *testing.T) {
	ctx := deadlineOnlyContext{deadline: time.Now().Add(time.Hour)}
	err := status.Error(codes.DeadlineExceeded, "context deadline exceeded")

	if shouldIgnoreCancellation(ctx, err) {
		t.Fatal("deadline error before measurement deadline should be counted")
	}
}

type deadlineOnlyContext struct {
	deadline time.Time
}

func (c deadlineOnlyContext) Deadline() (time.Time, bool) {
	return c.deadline, true
}

func (deadlineOnlyContext) Done() <-chan struct{} {
	return nil
}

func (deadlineOnlyContext) Err() error {
	return nil
}

func (deadlineOnlyContext) Value(key any) any {
	return nil
}

var _ context.Context = deadlineOnlyContext{}
