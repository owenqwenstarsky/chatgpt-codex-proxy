package accountmanager

import (
	"context"
	"errors"
	"sync"
)

const DefaultMaxActiveRequestsPerAccount = 2

var ErrCapacityClosed = errors.New("account capacity coordinator closed")

type CapacitySnapshot struct {
	Active int
	Queued int
	Limit  int
}

type capacityWaiter struct {
	ready   chan struct{}
	granted bool
}

type accountCapacity struct {
	active  int
	waiters []*capacityWaiter
}

// CapacityCoordinator owns process-local account slots. Waiters are granted in
// FIFO order for each account.
type CapacityCoordinator struct {
	mu       sync.Mutex
	limit    int
	closed   bool
	accounts map[string]*accountCapacity
}

func NewCapacityCoordinator(limit int) *CapacityCoordinator {
	if limit <= 0 {
		limit = DefaultMaxActiveRequestsPerAccount
	}
	return &CapacityCoordinator{limit: limit, accounts: make(map[string]*accountCapacity)}
}

func (c *CapacityCoordinator) HasCapacity(accountID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.accounts[accountID]
	return !c.closed && (state == nil || state.active < c.limit) && (state == nil || len(state.waiters) == 0)
}

func (c *CapacityCoordinator) TryAcquire(accountID string) (func(), bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.stateLocked(accountID)
	if c.closed || state.active >= c.limit || len(state.waiters) > 0 {
		return nil, false
	}
	state.active++
	return c.releaseFunc(accountID), true
}

func (c *CapacityCoordinator) Acquire(ctx context.Context, accountID string) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrCapacityClosed
	}
	state := c.stateLocked(accountID)
	if state.active < c.limit && len(state.waiters) == 0 {
		state.active++
		c.mu.Unlock()
		return c.releaseFunc(accountID), nil
	}
	waiter := &capacityWaiter{ready: make(chan struct{})}
	state.waiters = append(state.waiters, waiter)
	c.mu.Unlock()

	select {
	case <-waiter.ready:
		c.mu.Lock()
		granted := waiter.granted
		closed := c.closed
		c.mu.Unlock()
		if granted {
			return c.releaseFunc(accountID), nil
		}
		if closed {
			return nil, ErrCapacityClosed
		}
		return nil, context.Canceled
	case <-ctx.Done():
		c.mu.Lock()
		if waiter.granted {
			c.mu.Unlock()
			return c.releaseFunc(accountID), nil
		}
		state := c.accounts[accountID]
		if state != nil {
			for i, queued := range state.waiters {
				if queued == waiter {
					state.waiters = append(state.waiters[:i], state.waiters[i+1:]...)
					break
				}
			}
			c.cleanupLocked(accountID, state)
		}
		c.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (c *CapacityCoordinator) Snapshot(accountID string) CapacitySnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := CapacitySnapshot{Limit: c.limit}
	if state := c.accounts[accountID]; state != nil {
		result.Active = state.active
		result.Queued = len(state.waiters)
	}
	return result
}

func (c *CapacityCoordinator) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	for _, state := range c.accounts {
		for _, waiter := range state.waiters {
			close(waiter.ready)
		}
		state.waiters = nil
	}
}

func (c *CapacityCoordinator) stateLocked(id string) *accountCapacity {
	state := c.accounts[id]
	if state == nil {
		state = &accountCapacity{}
		c.accounts[id] = state
	}
	return state
}

func (c *CapacityCoordinator) releaseFunc(accountID string) func() {
	var once sync.Once
	return func() { once.Do(func() { c.release(accountID) }) }
}

func (c *CapacityCoordinator) release(accountID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.accounts[accountID]
	if state == nil || state.active == 0 {
		return
	}
	state.active--
	if !c.closed && len(state.waiters) > 0 && state.active < c.limit {
		waiter := state.waiters[0]
		state.waiters = state.waiters[1:]
		state.active++
		waiter.granted = true
		close(waiter.ready)
	}
	c.cleanupLocked(accountID, state)
}

func (c *CapacityCoordinator) cleanupLocked(id string, state *accountCapacity) {
	if state.active == 0 && len(state.waiters) == 0 {
		delete(c.accounts, id)
	}
}
