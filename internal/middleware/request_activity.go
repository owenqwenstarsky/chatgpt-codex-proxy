package middleware

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"chatgpt-codex-proxy/internal/activity"
)

const requestActivityStoreKey = "request_activity_store"

var trackedRequestPaths = map[string]struct{}{
	"/v1/completions":        {},
	"/v1/chat/completions":   {},
	"/v1/responses":          {},
	"/v1/responses/compact":  {},
	"/v1/images/generations": {},
	"/v1/images/edits":       {},
	"/v1/messages":           {},
}

func RequestActivity(store *activity.Store, logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		if store == nil || !tracksActivity(c.Request.Method, c.Request.URL.Path) {
			c.Next()
			return
		}

		requestID := GetRequestID(c)
		c.Set(requestActivityStoreKey, store)
		store.Start(requestID, c.Request.URL.Path)
		c.Next()

		route := c.FullPath()
		if route == "" {
			route = c.Request.URL.Path
		}
		status := c.Writer.Status()
		outcome := activityOutcome(c.GetString(RequestOutcomeKey), status)
		if err := store.Finish(
			requestID,
			route,
			status,
			outcome,
			c.GetString(RequestErrorCodeKey),
			c.GetString(RequestErrorMessageKey),
		); err != nil && logger != nil {
			logger.Error("persist request activity failed", "request_id", requestID, "error", err.Error())
		}
	}
}

func SetRequestActivityModel(c *gin.Context, model string) {
	updateRequestActivity(c, func(record *activity.Record) {
		record.Model = truncateActivityValue(model)
	})
}

func SetRequestActivityAccount(c *gin.Context, accountID, accountLabel string) {
	updateRequestActivity(c, func(record *activity.Record) {
		record.AccountID = truncateActivityValue(accountID)
		record.AccountLabel = truncateActivityValue(accountLabel)
		record.Phase = activity.PhaseUpstream
	})
}

func SetRequestActivityPhase(c *gin.Context, phase activity.Phase) {
	updateRequestActivity(c, func(record *activity.Record) {
		record.Phase = phase
	})
}

func updateRequestActivity(c *gin.Context, update func(*activity.Record)) {
	if c == nil || update == nil {
		return
	}
	value, ok := c.Get(requestActivityStoreKey)
	if !ok {
		return
	}
	store, ok := value.(*activity.Store)
	if !ok || store == nil {
		return
	}
	store.Update(GetRequestID(c), update)
}

func tracksActivity(method, path string) bool {
	if _, ok := trackedRequestPaths[path]; !ok {
		return false
	}
	return method == http.MethodPost
}

func SetRequestActivityStreaming(c *gin.Context) {
	SetRequestActivityPhase(c, activity.PhaseStreaming)
}

func SetRequestActivityFinalizing(c *gin.Context) {
	SetRequestActivityPhase(c, activity.PhaseFinalizing)
}

func activityOutcome(value string, status int) activity.Outcome {
	switch strings.TrimSpace(value) {
	case "client_canceled":
		return activity.OutcomeCancelled
	case "request_timeout":
		return activity.OutcomeTimedOut
	case "upstream_error", "stream_error", "server_error", "request_error":
		return activity.OutcomeFailed
	}
	if status >= http.StatusBadRequest {
		return activity.OutcomeFailed
	}
	return activity.OutcomeSucceeded
}

func truncateActivityValue(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 200 {
		return value[:200] + "…"
	}
	return value
}
