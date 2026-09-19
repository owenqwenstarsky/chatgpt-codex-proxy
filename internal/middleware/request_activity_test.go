package middleware

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"chatgpt-codex-proxy/internal/activity"
)

func TestRequestActivityTracksOnlyConfiguredMethodPaths(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := activity.NewStore(t.TempDir(), activity.Options{})
	engine := gin.New()
	engine.Use(RequestID(), RequestActivity(store, slog.New(slog.NewTextHandler(io.Discard, nil))))
	engine.POST("/v1/responses", func(c *gin.Context) {
		SetActivityModel(c, "gpt-5.6-terra")
		SetActivityAccount(c, "acct_01", "primary")
		MarkActivityStreaming(c)
		MarkActivityFinalizing(c)
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})
	engine.GET("/v1/responses", func(c *gin.Context) { c.Status(http.StatusSwitchingProtocols) })
	engine.GET("/health", func(c *gin.Context) { c.Status(http.StatusOK) })

	request := httptest.NewRequest(http.MethodPost, "/v1/responses?ignored=1", nil)
	request.Header.Set("X-Request-Id", "req_custom")
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
	snapshot := store.Snapshot()
	if len(snapshot.Requests) != 1 {
		t.Fatalf("requests = %#v", snapshot.Requests)
	}
	record := snapshot.Requests[0]
	if record.ID != "req_custom" || record.Route != "/v1/responses" || record.Model != "gpt-5.6-terra" || record.AccountID != "acct_01" || record.Phase != activity.PhaseComplete || record.Outcome != activity.OutcomeSucceeded {
		t.Fatalf("record = %#v", record)
	}

	for _, target := range []string{"/v1/responses", "/health"} {
		recorder = httptest.NewRecorder()
		engine.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, target, nil))
	}
	if len(store.Snapshot().Requests) != 1 {
		t.Fatal("excluded routes were tracked")
	}
}

func TestRequestActivityTrackedRouteMatrix(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := activity.NewStore(t.TempDir(), activity.Options{})
	engine := gin.New()
	engine.Use(RequestID(), RequestActivity(store, nil))
	tracked := []string{
		"/v1/completions",
		"/v1/chat/completions",
		"/v1/responses",
		"/v1/responses/compact",
		"/v1/images/generations",
		"/v1/images/edits",
		"/v1/messages",
	}
	for _, route := range tracked {
		engine.POST(route, func(c *gin.Context) { c.Status(http.StatusOK) })
	}
	engine.POST("/v1/messages/count_tokens", func(c *gin.Context) { c.Status(http.StatusOK) })
	engine.GET("/v1/models", func(c *gin.Context) { c.Status(http.StatusOK) })
	engine.GET("/admin/requests/activity", func(c *gin.Context) { c.Status(http.StatusOK) })

	for _, route := range tracked {
		engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, route, nil))
	}
	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/responses", nil))
	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", nil))
	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/admin/requests/activity", nil))

	snapshot := store.Snapshot()
	if len(snapshot.Requests) != len(tracked) {
		t.Fatalf("tracked requests = %d, want %d: %#v", len(snapshot.Requests), len(tracked), snapshot.Requests)
	}
	seen := make(map[string]bool)
	for _, record := range snapshot.Requests {
		seen[record.Route] = true
	}
	for _, route := range tracked {
		if !seen[route] {
			t.Fatalf("route %q was not tracked", route)
		}
	}
}

func TestRequestActivityClassifiesCancellationAndDoesNotChangeResponseOnWriteFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	parent := t.TempDir()
	notDirectory := filepath.Join(parent, "file")
	if err := os.WriteFile(notDirectory, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := activity.NewStore(notDirectory, activity.Options{})
	engine := gin.New()
	engine.Use(RequestID(), RequestActivity(store, slog.New(slog.NewTextHandler(io.Discard, nil))))
	engine.POST("/v1/responses", func(c *gin.Context) { c.Status(http.StatusNoContent) })

	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/responses", nil))
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", recorder.Code)
	}
	written := store.Snapshot().Requests[0]
	if written.Outcome != activity.OutcomeSucceeded || written.Status == nil || *written.Status != http.StatusNoContent {
		t.Fatalf("record = %#v", written)
	}

	cancelStore := activity.NewStore(t.TempDir(), activity.Options{})
	cancelEngine := gin.New()
	cancelEngine.Use(RequestID(), RequestActivity(cancelStore, nil))
	cancelEngine.POST("/v1/responses", func(*gin.Context) {})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(ctx)
	cancelEngine.ServeHTTP(httptest.NewRecorder(), request)
	record := cancelStore.Snapshot().Requests[0]
	if record.Outcome != activity.OutcomeCancelled || record.Status != nil || record.ErrorMessage != "client canceled request" {
		t.Fatalf("cancel record = %#v", record)
	}
}

func TestRequestActivityClassifiesHTTPFailureAndDeadline(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := activity.NewStore(t.TempDir(), activity.Options{})
	engine := gin.New()
	engine.Use(RequestID(), RequestActivity(store, nil))
	engine.POST("/v1/responses", func(c *gin.Context) {
		SetRequestError(c, "model_not_found", "raw model and prompt secret")
		c.Status(http.StatusNotFound)
	})
	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/responses", nil))
	record := store.Snapshot().Requests[0]
	if record.Outcome != activity.OutcomeFailed || record.ErrorCode != "model_not_found" || record.ErrorMessage != "model not found" || strings.Contains(record.ErrorMessage, "secret") {
		t.Fatalf("failure record = %#v", record)
	}

	deadlineStore := activity.NewStore(t.TempDir(), activity.Options{})
	deadlineEngine := gin.New()
	deadlineEngine.Use(RequestID(), RequestActivity(deadlineStore, nil))
	deadlineEngine.POST("/v1/responses", func(*gin.Context) {})
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	deadlineEngine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(ctx))
	record = deadlineStore.Snapshot().Requests[0]
	if record.Outcome != activity.OutcomeTimedOut || record.ErrorCode != "request_timeout" || record.ErrorMessage != "request timed out" {
		t.Fatalf("deadline record = %#v", record)
	}
}
