package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"chatgpt-codex-proxy/internal/activity"
)

const activityHeartbeatInterval = 15 * time.Second

func (a *App) handleAdminRequestActivity(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, a.activity.Snapshot())
}

func (a *App) handleAdminRequestActivityStream(c *gin.Context) {
	snapshot, events, cancel := a.activity.Subscribe()
	defer cancel()

	headers := c.Writer.Header()
	headers.Set("Content-Type", "text/event-stream; charset=utf-8")
	headers.Set("Cache-Control", "no-cache, no-transform")
	headers.Set("Connection", "keep-alive")
	headers.Set("X-Accel-Buffering", "no")
	c.Status(http.StatusOK)
	writeActivityEvent(c, "snapshot", activity.Event{Type: "snapshot", Snapshot: &snapshot})

	heartbeat := time.NewTicker(activityHeartbeatInterval)
	defer heartbeat.Stop()
	for {
		select {
		case <-c.Request.Context().Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			writeActivityEvent(c, event.Type, event)
		case <-heartbeat.C:
			_, _ = io.WriteString(c.Writer, ": keep-alive\n\n")
			c.Writer.Flush()
		}
	}
}

func (a *App) handleAdminRequestLogDates(c *gin.Context) {
	days, err := a.activity.Dates()
	if err != nil {
		a.writeAdminError(c, http.StatusInternalServerError, "request_logs_failed", err.Error())
		return
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, gin.H{
		"days":      days,
		"fetchedAt": time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func (a *App) handleAdminRequestLogs(c *gin.Context) {
	date := strings.TrimSpace(c.Query("date"))
	parsedDate, err := time.Parse(time.DateOnly, date)
	if err != nil || parsedDate.Format(time.DateOnly) != date {
		a.writeAdminError(c, http.StatusBadRequest, "invalid_date", "date must be YYYY-MM-DD")
		return
	}
	limit := 50
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 100 {
			a.writeAdminError(c, http.StatusBadRequest, "invalid_limit", "limit must be between 1 and 100")
			return
		}
	}
	outcome := activity.Outcome(strings.TrimSpace(c.Query("outcome")))
	if outcome != "" && !validLogOutcome(outcome) {
		a.writeAdminError(c, http.StatusBadRequest, "invalid_outcome", "invalid outcome filter")
		return
	}
	for _, value := range []string{c.Query("cursor"), c.Query("q"), c.Query("account"), c.Query("model")} {
		if len(value) > 200 {
			a.writeAdminError(c, http.StatusBadRequest, "invalid_filter", "a filter value is too long")
			return
		}
	}
	page, err := a.activity.Logs(activity.LogQuery{
		Date:    date,
		Limit:   limit,
		Cursor:  c.Query("cursor"),
		Query:   c.Query("q"),
		Outcome: outcome,
		Account: c.Query("account"),
		Model:   c.Query("model"),
	})
	if err != nil {
		status := http.StatusInternalServerError
		code := "request_logs_failed"
		if err.Error() == "invalid cursor" {
			status = http.StatusBadRequest
			code = "invalid_cursor"
		}
		a.writeAdminError(c, status, code, err.Error())
		return
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, page)
}

func writeActivityEvent(c *gin.Context, eventName string, event activity.Event) {
	payload, err := json.Marshal(event)
	if err != nil {
		return
	}
	writeSSE(c.Writer, eventName, payload)
	c.Writer.Flush()
}

func validLogOutcome(outcome activity.Outcome) bool {
	switch outcome {
	case activity.OutcomeActive, activity.OutcomeSucceeded, activity.OutcomeFailed, activity.OutcomeCancelled, activity.OutcomeTimedOut:
		return true
	default:
		return false
	}
}
