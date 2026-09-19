package middleware

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"chatgpt-codex-proxy/internal/activity"
)

func TestRequestActivityTracksOnlyPublicInferenceRequests(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)
	store, err := activity.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	engine := gin.New()
	engine.Use(RequestID())
	engine.Use(RequestActivity(store, slog.New(slog.NewTextHandler(io.Discard, nil))))
	engine.POST("/v1/responses", func(c *gin.Context) {
		SetRequestActivityModel(c, "gpt-6-astra")
		SetRequestActivityAccount(c, "acct_1", "primary")
		SetRequestActivityStreaming(c)
		SetRequestError(c, "upstream_error", "Bearer must-not-be-recorded")
		SetRequestOutcome(c, "upstream_error")
		c.JSON(http.StatusBadGateway, gin.H{"error": "failed"})
	})
	engine.GET("/v1/responses", func(c *gin.Context) { c.Status(http.StatusSwitchingProtocols) })
	engine.GET("/health", func(c *gin.Context) { c.Status(http.StatusOK) })

	request := func(method, path string) {
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(method, path, nil)
		engine.ServeHTTP(recorder, req)
	}
	request(http.MethodPost, "/v1/responses")
	request(http.MethodGet, "/v1/responses")
	request(http.MethodGet, "/health")

	snapshot := store.Snapshot()
	if len(snapshot.Requests) != 1 {
		t.Fatalf("tracked requests = %d, want 1", len(snapshot.Requests))
	}
	record := snapshot.Requests[0]
	if record.Route != "/v1/responses" || record.Model != "gpt-6-astra" || record.AccountLabel != "primary" {
		t.Fatalf("unexpected record: %#v", record)
	}
	if record.Status != http.StatusBadGateway || record.Outcome != activity.OutcomeFailed || record.Phase != activity.PhaseComplete {
		t.Fatalf("unexpected completion: %#v", record)
	}
	if record.ErrorMessage != "upstream request failed" {
		t.Fatalf("unsafe error message: %q", record.ErrorMessage)
	}
}
