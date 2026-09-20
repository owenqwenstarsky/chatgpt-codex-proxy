package accountmanager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"chatgpt-codex-proxy/internal/accounts"
	"chatgpt-codex-proxy/internal/codex"
	"chatgpt-codex-proxy/internal/codexauth"
	"chatgpt-codex-proxy/internal/config"
)

type AccountManager struct {
	cfg      config.Config
	accounts *accounts.Service
	oauth    *codexauth.OAuthService
	http     *codex.HTTPClient
	models   func(accounts.Record, string) bool

	locks    sync.Map
	capacity *CapacityCoordinator
}

type Lease struct {
	Account accounts.Record
	release func()
}

func (l *Lease) Release() {
	if l != nil && l.release != nil {
		l.release()
	}
}

// ErrAccountNotFound indicates that the requested local account does not exist.
var ErrAccountNotFound = errors.New("account not found")

func NewAccountManager(cfg config.Config, accountsSvc *accounts.Service, oauth *codexauth.OAuthService, httpClient *codex.HTTPClient, modelSupport func(accounts.Record, string) bool) *AccountManager {
	return &AccountManager{
		cfg:      cfg,
		accounts: accountsSvc,
		oauth:    oauth,
		http:     httpClient,
		models:   modelSupport,
		capacity: NewCapacityCoordinator(cfg.MaxActiveRequestsPerAccount),
	}
}

func (m *AccountManager) Close() { m.capacity.Close() }

func (m *AccountManager) Capacity(id string) CapacitySnapshot { return m.capacity.Snapshot(id) }
func (m *AccountManager) HasCapacity(id string) bool          { return m.capacity.HasCapacity(id) }

func (m *AccountManager) AcquireReadyLease(ctx context.Context, preferredID string) (*Lease, error) {
	return m.AcquireMatchingLease(ctx, preferredID, nil)
}

func (m *AccountManager) AcquireReadyForModelLease(ctx context.Context, preferredID, modelID string) (*Lease, error) {
	return m.AcquireMatchingLease(ctx, preferredID, func(record accounts.Record) bool {
		return m.models == nil || m.models(record, modelID)
	})
}

// AcquireMatchingLease prefers an eligible matching account with immediately
// available capacity. If all matching accounts are full, normal rotation picks
// the account whose FIFO queue receives the request.
func (m *AccountManager) AcquireMatchingLease(ctx context.Context, preferredID string, allow func(accounts.Record) bool) (*Lease, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		record, err := m.accounts.AcquireMatching(preferredID, func(candidate accounts.Record) bool {
			return (allow == nil || allow(candidate)) && m.capacity.HasCapacity(candidate.ID)
		})
		if err == nil {
			release, acquired := m.capacity.TryAcquire(record.ID)
			if !acquired {
				continue
			}
			ready, readyErr := m.ensureReady(ctx, record.ID)
			if readyErr != nil {
				release()
				return nil, readyErr
			}
			return &Lease{Account: ready, release: release}, nil
		}

		// Distinguish "all matching accounts are full" from no eligible
		// capacity. The latter must retain the existing no-active-account error.
		record, err = m.accounts.AcquireMatching(preferredID, allow)
		if err != nil {
			return nil, err
		}
		release, err := m.capacity.Acquire(ctx, record.ID)
		if err != nil {
			return nil, err
		}
		eligible, eligibleErr := m.accounts.EligibleNow(record.ID)
		if eligibleErr != nil {
			release()
			return nil, eligibleErr
		}
		updated, ok, getErr := m.accounts.Get(record.ID)
		if getErr != nil {
			release()
			return nil, getErr
		}
		if !ok || !eligible || (allow != nil && !allow(updated)) {
			release()
			continue
		}
		ready, readyErr := m.ensureReady(ctx, record.ID)
		if readyErr != nil {
			release()
			return nil, readyErr
		}
		return &Lease{Account: ready, release: release}, nil
	}
}

// AcquireSpecificLease waits for one required account. When requireEligible is
// false it preserves continuation behavior, where cooldown alone cannot move a
// stateful request to another account.
func (m *AccountManager) AcquireSpecificLease(ctx context.Context, id string, requireEligible bool) (*Lease, error) {
	if _, err := m.getRecord(id); err != nil {
		return nil, err
	}
	release, err := m.capacity.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	if requireEligible {
		eligible, eligibleErr := m.accounts.EligibleNow(id)
		if eligibleErr != nil {
			release()
			return nil, eligibleErr
		}
		if !eligible {
			release()
			return nil, fmt.Errorf("account unavailable")
		}
	}
	ready, err := m.ensureReady(ctx, id)
	if err != nil {
		release()
		return nil, err
	}
	return &Lease{Account: ready, release: release}, nil
}

func (m *AccountManager) ensureReady(ctx context.Context, id string) (accounts.Record, error) {
	record, err := m.getRecord(id)
	if err != nil {
		return accounts.Record{}, err
	}
	if err := validateReadyRecord(record); err != nil {
		return accounts.Record{}, err
	}
	if time.Until(record.Token.ExpiresAt) > m.cfg.RefreshSkew {
		return record, nil
	}

	lock := m.lockFor(id)
	lock.Lock()
	defer lock.Unlock()

	record, err = m.getRecord(id)
	if err != nil {
		return accounts.Record{}, err
	}
	if err := validateReadyRecord(record); err != nil {
		return accounts.Record{}, err
	}
	if time.Until(record.Token.ExpiresAt) > m.cfg.RefreshSkew {
		return record, nil
	}

	return m.refreshLocked(ctx, record)
}

// EnsureReady is retained for non-upstream callers that only need token
// validation. Upstream operations should acquire a lease first.
func (m *AccountManager) EnsureReady(ctx context.Context, id string) (accounts.Record, error) {
	return m.ensureReady(ctx, id)
}

func (m *AccountManager) Refresh(ctx context.Context, id string) (accounts.Record, error) {
	release, err := m.capacity.Acquire(ctx, id)
	if err != nil {
		return accounts.Record{}, err
	}
	defer release()
	lock := m.lockFor(id)
	lock.Lock()
	defer lock.Unlock()

	record, err := m.getRecord(id)
	if err != nil {
		return accounts.Record{}, err
	}
	return m.refreshLocked(ctx, record)
}

func (m *AccountManager) GetUsage(ctx context.Context, id string, cached bool) (accounts.Record, *accounts.QuotaSnapshot, error) {
	if cached {
		record, ok, err := m.accounts.Get(id)
		if err != nil {
			return accounts.Record{}, nil, err
		}
		if !ok {
			return accounts.Record{}, nil, ErrAccountNotFound
		}
		return record, record.CachedQuota, nil
	}

	lease, err := m.AcquireSpecificLease(ctx, id, true)
	if err != nil {
		return accounts.Record{}, nil, err
	}
	defer lease.Release()
	record := lease.Account

	_, quota, err := m.http.GetUsage(ctx, record)
	if err != nil {
		return record, nil, err
	}
	if err := m.accounts.ObserveQuota(record.ID, quota); err != nil {
		return record, nil, err
	}
	updated, ok, err := m.accounts.Get(record.ID)
	if err != nil {
		return accounts.Record{}, nil, err
	}
	if !ok {
		return accounts.Record{}, nil, fmt.Errorf("account %q disappeared after quota update", record.ID)
	}
	return updated, quota, nil
}

func (m *AccountManager) markRefreshFailure(id string, cause error) {
	var err error
	if codexauth.IsTerminalCredentialFailure(cause) {
		err = m.accounts.MarkError(id, accounts.StatusExpired, cause.Error())
	} else {
		until := time.Now().UTC().Add(accounts.DefaultRateLimitFallback)
		err = m.accounts.SetCooldown(id, &until, cause.Error())
	}
	if err != nil {
		slog.Default().Error("persist account refresh failure status failed",
			"account_id", id,
			"refresh_error", cause.Error(),
			"error", err.Error(),
		)
	}
}

func (m *AccountManager) refreshLocked(ctx context.Context, record accounts.Record) (accounts.Record, error) {
	nextToken, nextAccountID, err := m.oauth.Refresh(ctx, record.Token, record.AccountID)
	if err != nil {
		m.markRefreshFailure(record.ID, err)
		return accounts.Record{}, err
	}
	if err := m.accounts.UpdateAuth(record.ID, nextAccountID, nextToken); err != nil {
		return accounts.Record{}, err
	}
	updated, err := m.getRecord(record.ID)
	if err != nil {
		if errors.Is(err, ErrAccountNotFound) {
			return accounts.Record{}, fmt.Errorf("account %q disappeared after auth update", record.ID)
		}
		return accounts.Record{}, err
	}
	return updated, nil
}

func (m *AccountManager) getRecord(id string) (accounts.Record, error) {
	record, ok, err := m.accounts.Get(id)
	if err != nil {
		return accounts.Record{}, err
	}
	if !ok {
		return accounts.Record{}, ErrAccountNotFound
	}
	return record, nil
}

func validateReadyRecord(record accounts.Record) error {
	if record.Status == accounts.StatusDisabled {
		return fmt.Errorf("account disabled")
	}
	if record.Token.AccessToken == "" {
		return fmt.Errorf("account has no access token")
	}
	return nil
}

func (m *AccountManager) lockFor(id string) *sync.Mutex {
	lock, _ := m.locks.LoadOrStore(id, &sync.Mutex{})
	return lock.(*sync.Mutex)
}
