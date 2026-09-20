package accountmanager

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCapacityCoordinatorFIFOAndIdempotentRelease(t *testing.T) {
	c := NewCapacityCoordinator(1)
	release, err := c.Acquire(context.Background(), "acct")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	done := make(chan func())
	go func() {
		close(started)
		next, acquireErr := c.Acquire(context.Background(), "acct")
		if acquireErr != nil {
			t.Errorf("queued acquire: %v", acquireErr)
			return
		}
		done <- next
	}()
	<-started
	time.Sleep(10 * time.Millisecond)
	if got := c.Snapshot("acct"); got.Active != 1 || got.Queued != 1 {
		t.Fatalf("snapshot = %#v", got)
	}
	release()
	release()
	select {
	case next := <-done:
		next()
	case <-time.After(time.Second):
		t.Fatal("queued request was not released")
	}
}

func TestCapacityCoordinatorCancellationAndClose(t *testing.T) {
	c := NewCapacityCoordinator(1)
	release, err := c.Acquire(context.Background(), "acct")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, acquireErr := c.Acquire(ctx, "acct"); result <- acquireErr }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	if got := c.Snapshot("acct"); got.Queued != 0 {
		t.Fatalf("queued = %d", got.Queued)
	}
	release()
	release()
	release, err = c.Acquire(context.Background(), "acct")
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { _, acquireErr := c.Acquire(context.Background(), "acct"); closed <- acquireErr }()
	time.Sleep(10 * time.Millisecond)
	c.Close()
	if err := <-closed; !errors.Is(err, ErrCapacityClosed) {
		t.Fatalf("close error = %v", err)
	}
	release()
}
