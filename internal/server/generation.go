package server

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"chatgpt-codex-proxy/internal/accounts"
	"chatgpt-codex-proxy/internal/generation"
	"chatgpt-codex-proxy/internal/middleware"
	"github.com/gin-gonic/gin"
)

const generationIDsKey = "generation_attempt_ids"

func (a *App) startGeneration(c *gin.Context, endpoint, transport string, account accounts.Record, model string, attempt int, payload any) string {
	if a == nil || a.generations == nil {
		return ""
	}
	r, ok := a.generations.Start(generation.StartInput{ParentRequestID: middleware.GetRequestID(c), Endpoint: endpoint, Transport: transport, AccountID: account.ID, AccountLabel: account.Label, UpstreamAccountID: account.AccountID, Model: model, Attempt: attempt, Payload: payload})
	if !ok {
		return ""
	}
	ids := c.GetStringSlice(generationIDsKey)
	ids = append(ids, r.ID)
	c.Set(generationIDsKey, ids)
	return r.ID
}

func (a *App) finishGeneration(id string, outcome generation.Outcome, status int, err error, responseID string) {
	a.finishGenerationSummary(id, outcome, status, err, responseID, "", "", nil)
}

func (a *App) finishGenerationSummary(id string, outcome generation.Outcome, status int, err error, responseID, responseModel, terminal string, usage map[string]any) {
	if id == "" || a == nil || a.generations == nil {
		return
	}
	var p *int
	if status > 0 {
		p = &status
	}
	code, msg := "", ""
	if err != nil {
		code = "upstream_error"
		msg = err.Error()
	}
	_, _, _ = a.generations.Finish(id, generation.FinishInput{Outcome: outcome, Status: p, ErrorCode: code, ErrorMessage: msg, ResponseID: responseID, ResponseModel: responseModel, TerminalEvent: terminal, Usage: usage})
}

func (a *App) finishGenerations(c *gin.Context) {
	if a == nil || a.generations == nil {
		return
	}
	out := generation.OutcomeSucceeded
	switch c.GetString(middleware.RequestOutcomeKey) {
	case "client_canceled":
		out = generation.OutcomeCancelled
	case "request_timeout":
		out = generation.OutcomeTimedOut
	case "upstream_error", "stream_error", "server_error", "request_error":
		out = generation.OutcomeFailed
	}
	status := c.Writer.Status()
	rid := c.GetString(middleware.RequestResponseIDKey)
	model := c.GetString(middleware.RequestResponseModelKey)
	terminal := c.GetString(middleware.RequestTerminalEventKey)
	var usage map[string]any
	if value, ok := c.Get(middleware.RequestUsageKey); ok {
		usage, _ = value.(map[string]any)
	}
	for _, id := range c.GetStringSlice(generationIDsKey) {
		a.finishGenerationSummary(id, out, status, nil, rid, model, terminal, usage)
	}
}

func (a *App) handleGenerationLogDates(c *gin.Context) {
	r, e := a.generations.Dates()
	if e != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "generation_logs_unavailable"})
		return
	}
	c.JSON(http.StatusOK, r)
}
func (a *App) handleGenerationLogDetail(c *gin.Context) {
	r, ok := a.generations.Get(strings.TrimSpace(c.Param("attempt_id")))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "generation_log_not_found"})
		return
	}
	c.JSON(http.StatusOK, r)
}
func (a *App) handleGenerationLogs(c *gin.Context) {
	date := strings.TrimSpace(c.Query("date"))
	if date == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "date is required"})
		return
	}
	if _, err := time.Parse(time.DateOnly, date); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_date"})
		return
	}
	q := generation.Query{Date: date, Cursor: c.Query("cursor"), ParentRequestID: c.Query("request_id"), Account: c.Query("account"), Model: c.Query("model"), Endpoint: c.Query("endpoint"), Transport: c.Query("transport"), Outcome: generation.Outcome(c.Query("outcome"))}
	if raw := c.Query("limit"); raw != "" {
		if _, err := fmt.Sscanf(raw, "%d", &q.Limit); err != nil || q.Limit < 0 || q.Limit > 500 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_limit"})
			return
		}
	}
	r, e := a.generations.Query(q)
	if e != nil {
		if errors.Is(e, generation.ErrInvalidCursor) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_cursor"})
			return
		}
		if errors.Is(e, os.ErrNotExist) {
			c.JSON(http.StatusOK, generation.Response{Date: date, Records: []generation.Record{}})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "generation_logs_unavailable"})
		return
	}
	for i := range r.Records {
		r.Records[i].Payload = generationPayloadPreview(r.Records[i].Payload)
	}
	c.JSON(http.StatusOK, r)
}

func generationPayloadPreview(payload any) any {
	m, ok := payload.(map[string]any)
	if !ok {
		return nil
	}
	out := map[string]any{}
	for _, key := range []string{"model", "stream", "service_tier", "previous_response_id", "type"} {
		if value, exists := m[key]; exists {
			out[key] = value
		}
	}
	if input, exists := m["input"]; exists {
		switch items := input.(type) {
		case []any:
			out["input_count"] = len(items)
		default:
			out["input_present"] = true
		}
	}
	if tools, exists := m["tools"]; exists {
		if items, ok := tools.([]any); ok {
			out["tool_count"] = len(items)
		}
	}
	return out
}
