package server

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"chatgpt-codex-proxy/internal/activity"
	"chatgpt-codex-proxy/internal/middleware"
)

func newActivityHTTPServer(t *testing.T) (*App, *httptest.Server) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	store := activity.NewStore(t.TempDir(), activity.Options{})
	app := &App{
		logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		activity:          store,
		activityHeartbeat: 25 * time.Millisecond,
	}
	engine := gin.New()
	protected := engine.Group("/")
	protected.Use(middleware.APIKeyWithUnauthorized("secret", func(c *gin.Context) {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid_api_key", "message": "authentication failed"})
	}))
	protected.GET("/admin/requests/activity", app.handleRequestActivity)
	protected.GET("/admin/requests/activity/stream", app.handleRequestActivityStream)
	protected.GET("/admin/requests/logs/dates", app.handleRequestLogDates)
	protected.GET("/admin/requests/logs", app.handleRequestLogs)
	server := httptest.NewServer(engine)
	t.Cleanup(func() {
		server.Close()
		store.Close()
	})
	return app, server
}

func TestActivityEndpointsAuthenticationSnapshotAndSSE(t *testing.T) {
	app, server := newActivityHTTPServer(t)

	response, err := http.Get(server.URL + "/admin/requests/activity")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", response.StatusCode)
	}

	for _, header := range []struct{ name, value string }{
		{"Authorization", "Bearer secret"},
		{"X-API-Key", "secret"},
	} {
		request, _ := http.NewRequest(http.MethodGet, server.URL+"/admin/requests/activity", nil)
		request.Header.Set(header.name, header.value)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		var snapshot activity.Snapshot
		decodeErr := json.NewDecoder(response.Body).Decode(&snapshot)
		response.Body.Close()
		if response.StatusCode != http.StatusOK || decodeErr != nil || snapshot.Requests == nil || snapshot.EmittedAt.IsZero() {
			t.Fatalf("snapshot status=%d body=%#v err=%v", response.StatusCode, snapshot, decodeErr)
		}
		if response.Header.Get("Cache-Control") != "no-store" {
			t.Fatalf("cache control = %q", response.Header.Get("Cache-Control"))
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/admin/requests/activity/stream", nil)
	request.Header.Set("Authorization", "Bearer secret")
	request.Header.Set("Accept", "text/event-stream")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.Header.Get("Content-Type") != "text/event-stream; charset=utf-8" || response.Header.Get("Cache-Control") != "no-cache, no-transform" || response.Header.Get("X-Accel-Buffering") != "no" {
		t.Fatalf("SSE headers = %#v", response.Header)
	}
	reader := bufio.NewReader(response.Body)
	frame := readSSEFrame(t, reader)
	if !strings.HasPrefix(frame, "event: snapshot\n") || !strings.Contains(frame, `"type":"snapshot"`) || !strings.Contains(frame, `"requests":[]`) {
		t.Fatalf("snapshot frame = %q", frame)
	}
	app.activity.Start("req_stream", "/v1/responses")
	frame = readSSEFrame(t, reader)
	if !strings.HasPrefix(frame, "event: upsert\n") || !strings.Contains(frame, `"id":"req_stream"`) {
		t.Fatalf("upsert frame = %q", frame)
	}
	frame = readSSEFrame(t, reader)
	if frame != ": keep-alive\n\n" {
		t.Fatalf("heartbeat = %q", frame)
	}
}

func TestRequestLogEndpointsValidationFiltersAndPagination(t *testing.T) {
	app, server := newActivityHTTPServer(t)
	date := time.Now().UTC().Format(time.DateOnly)
	for _, item := range []struct {
		id      string
		model   string
		account string
		outcome activity.Outcome
		code    string
	}{
		{"req_success", "gpt-5.6-terra", "acct_primary", activity.OutcomeSucceeded, ""},
		{"req_failure", "gpt-5.6-sol", "acct_backup", activity.OutcomeFailed, "upstream_error"},
	} {
		app.activity.Start(item.id, "/v1/responses")
		app.activity.SetModel(item.id, item.model)
		app.activity.SetAccount(item.id, item.account, strings.TrimPrefix(item.account, "acct_"))
		if _, _, err := app.activity.Finish(item.id, activity.FinishInput{Outcome: item.outcome, ErrorCode: item.code}); err != nil {
			t.Fatal(err)
		}
	}

	dates := authenticatedGet(t, server.URL+"/admin/requests/logs/dates")
	defer dates.Body.Close()
	if dates.StatusCode != http.StatusOK || dates.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("dates status=%d headers=%#v", dates.StatusCode, dates.Header)
	}
	var datesBody activity.DatesResponse
	if err := json.NewDecoder(dates.Body).Decode(&datesBody); err != nil || len(datesBody.Days) != 1 || datesBody.Days[0].Date != date || datesBody.Days[0].Failed != 1 {
		t.Fatalf("dates = %#v, err=%v", datesBody, err)
	}

	query := url.Values{"date": {date}, "limit": {"1"}, "q": {"REQ"}}
	page := authenticatedGet(t, server.URL+"/admin/requests/logs?"+query.Encode())
	var pageBody activity.LogResponse
	if err := json.NewDecoder(page.Body).Decode(&pageBody); err != nil {
		t.Fatal(err)
	}
	page.Body.Close()
	if page.StatusCode != http.StatusOK || len(pageBody.Records) != 1 || pageBody.NextCursor == nil {
		t.Fatalf("page = %#v status=%d", pageBody, page.StatusCode)
	}
	query.Set("cursor", *pageBody.NextCursor)
	next := authenticatedGet(t, server.URL+"/admin/requests/logs?"+query.Encode())
	var nextBody activity.LogResponse
	_ = json.NewDecoder(next.Body).Decode(&nextBody)
	next.Body.Close()
	if len(nextBody.Records) != 1 || nextBody.NextCursor != nil {
		t.Fatalf("next = %#v", nextBody)
	}

	filter := url.Values{"date": {date}, "outcome": {"failed"}, "account": {"BACKUP"}, "model": {"SOL"}, "q": {"upstream"}}
	filtered := authenticatedGet(t, server.URL+"/admin/requests/logs?"+filter.Encode())
	var filteredBody activity.LogResponse
	_ = json.NewDecoder(filtered.Body).Decode(&filteredBody)
	filtered.Body.Close()
	if len(filteredBody.Records) != 1 || filteredBody.Records[0].ID != "req_failure" {
		t.Fatalf("filtered = %#v", filteredBody)
	}

	invalid := []struct{ query, code string }{
		{"", "invalid_date"},
		{"?date=nope", "invalid_date"},
		{"?date=" + date + "&limit=0", "invalid_limit"},
		{"?date=" + date + "&outcome=nope", "invalid_outcome"},
		{"?date=" + date + "&q=" + strings.Repeat("x", 201), "invalid_filter"},
		{"?date=" + date + "&cursor=nope", "invalid_cursor"},
		{"?date=" + date + "&cursor=" + activity.EncodeCursor(999), "invalid_cursor"},
	}
	for _, test := range invalid {
		response := authenticatedGet(t, server.URL+"/admin/requests/logs"+test.query)
		var body map[string]any
		_ = json.NewDecoder(response.Body).Decode(&body)
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest || body["error"] != test.code {
			t.Fatalf("query %q status=%d body=%#v", test.query, response.StatusCode, body)
		}
	}

	missing := authenticatedGet(t, server.URL+"/admin/requests/logs?date=2026-01-01")
	var missingBody activity.LogResponse
	_ = json.NewDecoder(missing.Body).Decode(&missingBody)
	missing.Body.Close()
	if missing.StatusCode != http.StatusOK || missingBody.Records == nil || len(missingBody.Records) != 0 {
		t.Fatalf("missing = %#v status=%d", missingBody, missing.StatusCode)
	}
}

func authenticatedGet(t *testing.T, target string) *http.Response {
	t.Helper()
	request, _ := http.NewRequest(http.MethodGet, target, nil)
	request.Header.Set("Authorization", "Bearer secret")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func readSSEFrame(t *testing.T, reader *bufio.Reader) string {
	t.Helper()
	var frame strings.Builder
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read SSE: %v", err)
		}
		frame.WriteString(line)
		if line == "\n" {
			return frame.String()
		}
	}
}
