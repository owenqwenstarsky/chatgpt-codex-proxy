package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"chatgpt-codex-proxy/internal/activity"
)

func (a *App) handleRequestActivity(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, a.activity.Snapshot())
}

func (a *App) handleRequestActivityStream(c *gin.Context) {
	snapshot, subscription := a.activity.Subscribe()
	defer subscription.Close()

	headers := c.Writer.Header()
	headers.Set("Content-Type", "text/event-stream; charset=utf-8")
	headers.Set("Cache-Control", "no-cache, no-transform")
	headers.Set("Connection", "keep-alive")
	headers.Set("X-Accel-Buffering", "no")
	headers.Del("Content-Length")
	c.Status(http.StatusOK)
	c.Writer.WriteHeaderNow()

	first := activity.Event{Type: "snapshot", Snapshot: &snapshot}
	if err := writeActivitySSE(c, first); err != nil {
		return
	}

	interval := a.activityHeartbeat
	if interval <= 0 {
		interval = 15 * time.Second
	}
	heartbeat := time.NewTicker(interval)
	defer heartbeat.Stop()
	for {
		select {
		case <-c.Request.Context().Done():
			return
		case event, ok := <-subscription.Events:
			if !ok || writeActivitySSE(c, event) != nil {
				return
			}
		case <-heartbeat.C:
			if _, err := io.WriteString(c.Writer, ": keep-alive\n\n"); err != nil {
				return
			}
			c.Writer.Flush()
		}
	}
}

func (a *App) handleRequestLogDates(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	response, err := a.activity.ListDates()
	if err != nil {
		a.writeAdminError(c, http.StatusInternalServerError, "request_logs_failed", "request logs are unavailable")
		return
	}
	c.JSON(http.StatusOK, response)
}

func (a *App) handleRequestLogs(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	query, code, message := parseActivityLogQuery(c)
	if code != "" {
		a.writeAdminError(c, http.StatusBadRequest, code, message)
		return
	}
	response, err := a.activity.QueryLogs(query)
	if errors.Is(err, activity.ErrInvalidCursor) {
		a.writeAdminError(c, http.StatusBadRequest, "invalid_cursor", "cursor is invalid for this query")
		return
	}
	if err != nil {
		a.writeAdminError(c, http.StatusInternalServerError, "request_logs_failed", "request logs are unavailable")
		return
	}
	c.JSON(http.StatusOK, response)
}

func parseActivityLogQuery(c *gin.Context) (activity.LogQuery, string, string) {
	date := strings.TrimSpace(c.Query("date"))
	if !activity.ValidateDate(date) {
		return activity.LogQuery{}, "invalid_date", "date must be a real calendar date in YYYY-MM-DD format"
	}
	limit, err := activity.ParseLimit(strings.TrimSpace(c.Query("limit")))
	if err != nil {
		return activity.LogQuery{}, "invalid_limit", "limit must be an integer from 1 to 100"
	}
	outcome := strings.TrimSpace(c.Query("outcome"))
	if !activity.ValidateOutcome(outcome) {
		return activity.LogQuery{}, "invalid_outcome", "outcome is invalid"
	}
	values := map[string]string{
		"cursor":  c.Query("cursor"),
		"q":       c.Query("q"),
		"account": c.Query("account"),
		"model":   c.Query("model"),
	}
	for name, value := range values {
		if len([]rune(value)) > 200 {
			return activity.LogQuery{}, "invalid_filter", fmt.Sprintf("%s must be 200 characters or fewer", name)
		}
		values[name] = strings.TrimSpace(value)
	}
	if values["cursor"] != "" {
		if _, err := activity.DecodeCursor(values["cursor"]); err != nil {
			return activity.LogQuery{}, "invalid_cursor", "cursor is invalid for this query"
		}
	}
	return activity.LogQuery{
		Date:    date,
		Limit:   limit,
		Cursor:  values["cursor"],
		Q:       values["q"],
		Outcome: activity.Outcome(outcome),
		Account: values["account"],
		Model:   values["model"],
	}, "", ""
}

func writeActivitySSE(c *gin.Context, event activity.Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if _, err := io.WriteString(c.Writer, "event: "+event.Type+"\n"); err != nil {
		return err
	}
	if _, err := io.WriteString(c.Writer, "data: "); err != nil {
		return err
	}
	if _, err := c.Writer.Write(payload); err != nil {
		return err
	}
	if _, err := io.WriteString(c.Writer, "\n\n"); err != nil {
		return err
	}
	c.Writer.Flush()
	return nil
}
