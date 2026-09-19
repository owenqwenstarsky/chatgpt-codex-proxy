package middleware

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"chatgpt-codex-proxy/internal/activity"
)

const requestActivityKey = "request_activity"

type requestActivity struct {
	store *activity.Store
	id    string
}

var trackedInferenceRoutes = map[string]string{
	http.MethodPost + " /v1/completions":        "/v1/completions",
	http.MethodPost + " /v1/chat/completions":   "/v1/chat/completions",
	http.MethodPost + " /v1/responses":          "/v1/responses",
	http.MethodPost + " /v1/responses/compact":  "/v1/responses/compact",
	http.MethodPost + " /v1/images/generations": "/v1/images/generations",
	http.MethodPost + " /v1/images/edits":       "/v1/images/edits",
	http.MethodPost + " /v1/messages":           "/v1/messages",
}

func RequestActivity(store *activity.Store, logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		route, tracked := trackedInferenceRoutes[c.Request.Method+" "+c.Request.URL.Path]
		if !tracked || store == nil {
			c.Next()
			return
		}

		id := GetRequestID(c)
		if _, started := store.Start(id, route); !started {
			if logger != nil {
				logger.Warn("request activity start skipped", "request_id", id, "route", route)
			}
			c.Next()
			return
		}
		c.Set(requestActivityKey, &requestActivity{store: store, id: id})
		c.Next()

		if fullPath := strings.TrimSpace(c.FullPath()); fullPath != "" {
			store.SetRoute(id, fullPath)
		}
		outcome := classifyActivityOutcome(c)
		var status *int
		if c.Writer.Written() || (outcome != activity.OutcomeCancelled && outcome != activity.OutcomeTimedOut) {
			value := c.Writer.Status()
			status = &value
		}
		_, finished, err := store.Finish(id, activity.FinishInput{
			Outcome:   outcome,
			Status:    status,
			ErrorCode: c.GetString(RequestErrorCodeKey),
		})
		if err != nil && logger != nil {
			logger.Error("persist request activity failed", "request_id", id, "error", err.Error())
		}
		if !finished && logger != nil {
			logger.Warn("request activity finish skipped", "request_id", id)
		}
	}
}

func SetActivityModel(c *gin.Context, model string) {
	if tracker := getRequestActivity(c); tracker != nil {
		tracker.store.SetModel(tracker.id, model)
	}
}

func SetActivityAccount(c *gin.Context, accountID, accountLabel string) {
	if tracker := getRequestActivity(c); tracker != nil {
		tracker.store.SetAccount(tracker.id, accountID, accountLabel)
	}
}

func SetActivityPhase(c *gin.Context, phase activity.Phase) {
	if tracker := getRequestActivity(c); tracker != nil {
		tracker.store.SetPhase(tracker.id, phase)
	}
}

func MarkActivityStreaming(c *gin.Context) {
	SetActivityPhase(c, activity.PhaseStreaming)
}

func MarkActivityFinalizing(c *gin.Context) {
	SetActivityPhase(c, activity.PhaseFinalizing)
}

func getRequestActivity(c *gin.Context) *requestActivity {
	if c == nil {
		return nil
	}
	value, ok := c.Get(requestActivityKey)
	if !ok {
		return nil
	}
	tracker, _ := value.(*requestActivity)
	return tracker
}

func classifyActivityOutcome(c *gin.Context) activity.Outcome {
	switch c.GetString(RequestOutcomeKey) {
	case "client_canceled":
		return activity.OutcomeCancelled
	case "request_timeout":
		return activity.OutcomeTimedOut
	case "upstream_error", "stream_error", "server_error", "request_error":
		return activity.OutcomeFailed
	}
	if c.Request != nil {
		switch c.Request.Context().Err() {
		case context.Canceled:
			return activity.OutcomeCancelled
		case context.DeadlineExceeded:
			return activity.OutcomeTimedOut
		}
	}
	if c.GetString(RequestErrorCodeKey) != "" {
		return activity.OutcomeFailed
	}
	if c.Writer.Written() && c.Writer.Status() >= 400 {
		return activity.OutcomeFailed
	}
	return activity.OutcomeSucceeded
}
