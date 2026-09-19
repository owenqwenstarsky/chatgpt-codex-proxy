package server

import (
	"context"
	"math"
	"strings"
	"time"

	"chatgpt-codex-proxy/internal/accounts"
)

// rateLimitRecoveryError means all otherwise usable capacity remained
// temporarily unavailable through the configured in-request wait budget.
type rateLimitRecoveryError struct {
	RetryAt time.Time
}

func (e *rateLimitRecoveryError) Error() string {
	return "rate limited capacity did not recover before the wait budget expired"
}

func retryAfterSeconds(at time.Time) int {
	if at.IsZero() {
		return 1
	}
	return max(1, int(math.Ceil(time.Until(at).Seconds())))
}

// waitForCapacityRecovery waits only for capacity whose recovery is known from
// account cooldown/quota fields. It never inspects upstream error strings.
func (a *App) waitForCapacityRecovery(ctx context.Context, endpoint string, started time.Time, attempts int, allow func(accounts.Record) bool) (bool, error) {
	availability, err := a.accounts.EarliestAvailability(allow)
	if err != nil {
		return false, err
	}
	if availability.RecoveryAt == nil {
		return false, nil
	}

	now := time.Now().UTC()
	budgetEnd := started.Add(a.cfg.RateLimitMaxWait)
	waitUntil := *availability.RecoveryAt
	if waitUntil.After(budgetEnd) {
		waitUntil = budgetEnd
	}
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(waitUntil) {
		waitUntil = deadline
	}
	if !waitUntil.After(now) {
		if !availability.RecoveryAt.After(now) {
			return true, nil
		}
		return false, &rateLimitRecoveryError{RetryAt: *availability.RecoveryAt}
	}

	duration := time.Until(waitUntil)
	a.logger.Info("rate limit recovery wait",
		"endpoint", endpoint,
		"attempts", attempts,
		"wait_duration", duration.String(),
		"recovery_at", availability.RecoveryAt.Format(time.RFC3339),
	)
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		a.logger.Info("rate limit recovery canceled", "endpoint", endpoint, "attempts", attempts, "error", ctx.Err().Error())
		return false, ctx.Err()
	case <-timer.C:
	}
	if err := ctx.Err(); err != nil {
		a.logger.Info("rate limit recovery canceled", "endpoint", endpoint, "attempts", attempts, "error", err.Error())
		return false, err
	}
	if availability.RecoveryAt.After(time.Now().UTC()) {
		delay := retryAfterSeconds(*availability.RecoveryAt)
		a.logger.Info("rate limit recovery exhausted", "endpoint", endpoint, "attempts", attempts, "retry_after", delay)
		return false, &rateLimitRecoveryError{RetryAt: *availability.RecoveryAt}
	}
	a.logger.Info("rate limit recovery retry", "endpoint", endpoint, "attempts", attempts)
	return true, nil
}

func (a *App) recoveryAllowForResolution(resolution *sessionResolution) func(accounts.Record) bool {
	if resolution == nil {
		return nil
	}
	preferredID := strings.TrimSpace(resolution.PreferredAccountID)
	modelID := strings.TrimSpace(resolution.Request.Model)
	return func(record accounts.Record) bool {
		if (resolution.ExplicitPrevious || resolution.ImplicitResume) && record.ID != preferredID {
			return false
		}
		return modelID == "" || a.modelCatalog().SupportsRecord(record, modelID)
	}
}

func rateLimitRecoveryRetryAfter(err error) (int, bool) {
	var recovery *rateLimitRecoveryError
	if !asRateLimitRecoveryError(err, &recovery) {
		return 0, false
	}
	return retryAfterSeconds(recovery.RetryAt), true
}

func asRateLimitRecoveryError(err error, target **rateLimitRecoveryError) bool {
	for err != nil {
		if value, ok := err.(*rateLimitRecoveryError); ok {
			*target = value
			return true
		}
		unwrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			break
		}
		err = unwrapper.Unwrap()
	}
	return false
}
