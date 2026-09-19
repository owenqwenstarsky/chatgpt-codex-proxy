package accounts

import (
	"cmp"
	"slices"
	"strings"
	"time"
)

func selectRoundRobin(candidates []*Record, index *int) *Record {
	slices.SortFunc(candidates, func(a, b *Record) int { return strings.Compare(a.ID, b.ID) })
	selected := candidates[*index%len(candidates)]
	*index = *index + 1
	return selected
}

func selectLeastUsed(candidates []*Record, index *int) *Record {
	withQuota := make([]*Record, 0, len(candidates))
	withWindowDurations := make([]*Record, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate == nil || candidate.CachedQuota == nil || candidate.CachedQuota.RateLimit.UsedPercent == nil {
			continue
		}
		withQuota = append(withQuota, candidate)
		if len(rateLimitWindowsByDuration(candidate.CachedQuota)) > 0 {
			withWindowDurations = append(withWindowDurations, candidate)
		}
	}

	if len(withQuota) == 0 {
		return selectRoundRobin(candidates, index)
	}

	var sharedDurations []int
	if len(withWindowDurations) > 0 {
		withQuota = withWindowDurations
		sharedDurations = sharedRateLimitWindowDurations(withQuota)
		if len(sharedDurations) == 0 {
			return selectRoundRobin(withQuota, index)
		}
	}

	slices.SortFunc(withQuota, func(a, b *Record) int {
		return cmp.Or(compareLeastUsedQuota(a, b, sharedDurations), strings.Compare(a.ID, b.ID))
	})

	tiedCount := 1
	for tiedCount < len(withQuota) && compareLeastUsedQuota(withQuota[0], withQuota[tiedCount], sharedDurations) == 0 {
		tiedCount++
	}
	selected := withQuota[*index%tiedCount]
	*index = *index + 1
	return selected
}

func compareLeastUsedQuota(a, b *Record, sharedDurations []int) int {
	if len(sharedDurations) == 0 {
		return compareLegacyLeastUsedQuota(a.CachedQuota, b.CachedQuota)
	}

	aWindows := rateLimitWindowsByDuration(a.CachedQuota)
	bWindows := rateLimitWindowsByDuration(b.CachedQuota)
	for _, duration := range sharedDurations {
		aPercent := *aWindows[duration].UsedPercent
		bPercent := *bWindows[duration].UsedPercent
		switch {
		case aPercent < bPercent:
			return -1
		case aPercent > bPercent:
			return 1
		}
	}
	for _, duration := range sharedDurations {
		aReset := aWindows[duration].ResetAt
		bReset := bWindows[duration].ResetAt
		if aReset != nil && bReset != nil {
			if order := aReset.UTC().Compare(bReset.UTC()); order != 0 {
				return order
			}
		}
	}
	return 0
}

func compareLegacyLeastUsedQuota(aQuota, bQuota *QuotaSnapshot) int {
	aPrimary := primaryPercent(aQuota)
	bPrimary := primaryPercent(bQuota)
	switch {
	case aPrimary < bPrimary:
		return -1
	case aPrimary > bPrimary:
		return 1
	}

	aSecondary, aHasSecondary := secondaryPercent(aQuota)
	bSecondary, bHasSecondary := secondaryPercent(bQuota)
	if aHasSecondary && bHasSecondary {
		switch {
		case aSecondary < bSecondary:
			return -1
		case aSecondary > bSecondary:
			return 1
		}
	}

	aReset, aHasReset := primaryReset(aQuota)
	bReset, bHasReset := primaryReset(bQuota)
	if aHasReset && bHasReset {
		return aReset.Compare(bReset)
	}

	return 0
}

func rateLimitWindowsByDuration(snapshot *QuotaSnapshot) map[int]*RateLimitWindow {
	windows := make(map[int]*RateLimitWindow, 2)
	if snapshot == nil {
		return windows
	}
	for _, window := range []*RateLimitWindow{&snapshot.RateLimit, snapshot.SecondaryRateLimit} {
		if window == nil || window.UsedPercent == nil || window.LimitWindowSeconds == nil || *window.LimitWindowSeconds <= 0 {
			continue
		}
		if _, exists := windows[*window.LimitWindowSeconds]; !exists {
			windows[*window.LimitWindowSeconds] = window
		}
	}
	return windows
}

func sharedRateLimitWindowDurations(records []*Record) []int {
	if len(records) == 0 {
		return nil
	}
	shared := rateLimitWindowsByDuration(records[0].CachedQuota)
	for _, record := range records[1:] {
		windows := rateLimitWindowsByDuration(record.CachedQuota)
		for duration := range shared {
			if _, ok := windows[duration]; !ok {
				delete(shared, duration)
			}
		}
	}
	durations := make([]int, 0, len(shared))
	for duration := range shared {
		durations = append(durations, duration)
	}
	slices.Sort(durations)
	return durations
}

func primaryPercent(snapshot *QuotaSnapshot) float64 {
	if snapshot == nil || snapshot.RateLimit.UsedPercent == nil {
		return 0
	}
	return *snapshot.RateLimit.UsedPercent
}

func secondaryPercent(snapshot *QuotaSnapshot) (float64, bool) {
	if snapshot == nil || snapshot.SecondaryRateLimit == nil || snapshot.SecondaryRateLimit.UsedPercent == nil {
		return 0, false
	}
	return *snapshot.SecondaryRateLimit.UsedPercent, true
}

func primaryReset(snapshot *QuotaSnapshot) (time.Time, bool) {
	if snapshot == nil || snapshot.RateLimit.ResetAt == nil {
		return time.Time{}, false
	}
	return snapshot.RateLimit.ResetAt.UTC(), true
}

func normalizeQuotaSnapshot(snapshot *QuotaSnapshot, now time.Time) bool {
	if snapshot == nil {
		return false
	}
	primaryChanged := normalizeRateLimitWindow(&snapshot.RateLimit, now)
	secondaryChanged := normalizeRateLimitWindow(snapshot.SecondaryRateLimit, now)
	codeReviewChanged := normalizeRateLimitWindow(snapshot.CodeReviewRateLimit, now)
	return primaryChanged || secondaryChanged || codeReviewChanged
}

func normalizeRateLimitWindow(window *RateLimitWindow, now time.Time) bool {
	if window == nil || window.ResetAt == nil || window.ResetAt.After(now) {
		return false
	}
	window.Allowed = true
	window.LimitReached = false
	window.UsedPercent = nil
	window.ResetAt = nil
	return true
}

func quotaBlocksGeneralRouting(snapshot *QuotaSnapshot, now time.Time) bool {
	if snapshot == nil {
		return false
	}
	return windowAvailabilityBlocked(&snapshot.RateLimit, now) ||
		windowLimitActive(&snapshot.RateLimit, now) ||
		windowLimitActive(snapshot.SecondaryRateLimit, now)
}

func windowAvailabilityBlocked(window *RateLimitWindow, now time.Time) bool {
	if window == nil || window.Allowed {
		return false
	}
	if window.ResetAt == nil {
		return true
	}
	return window.ResetAt.After(now)
}

func windowLimitActive(window *RateLimitWindow, now time.Time) bool {
	if window == nil || !window.LimitReached {
		return false
	}
	if window.ResetAt == nil {
		return true
	}
	return window.ResetAt.After(now)
}

func isEligible(record *Record, now time.Time) bool {
	if record == nil || record.Status != StatusActive {
		return false
	}
	if strings.TrimSpace(record.Token.AccessToken) == "" {
		return false
	}
	if record.CooldownUntil != nil && record.CooldownUntil.After(now) {
		return false
	}
	return !quotaBlocksGeneralRouting(record.CachedQuota, now)
}

// recordRecoveryAt returns when every typed temporary blocker on a record has
// cleared. A blocker without a reset timestamp makes the recovery unknown.
func recordRecoveryAt(record *Record, now time.Time) *time.Time {
	if record == nil || record.Status != StatusActive || strings.TrimSpace(record.Token.AccessToken) == "" {
		return nil
	}
	var latest *time.Time
	add := func(value *time.Time) bool {
		if value == nil || !value.After(now) {
			return false
		}
		if latest == nil || value.After(*latest) {
			copy := value.UTC()
			latest = &copy
		}
		return true
	}
	blocked := false
	if record.CooldownUntil != nil && record.CooldownUntil.After(now) {
		blocked = true
		add(record.CooldownUntil)
	}
	if record.CachedQuota != nil {
		for _, window := range []*RateLimitWindow{&record.CachedQuota.RateLimit, record.CachedQuota.SecondaryRateLimit} {
			if window == nil || !(windowAvailabilityBlocked(window, now) || windowLimitActive(window, now)) {
				continue
			}
			blocked = true
			if !add(window.ResetAt) {
				return nil
			}
		}
	}
	if !blocked {
		return nil
	}
	return latest
}
