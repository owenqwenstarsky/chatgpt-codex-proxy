package server

import (
	"bufio"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"chatgpt-codex-proxy/internal/activity"
	"chatgpt-codex-proxy/internal/config"
)

func TestAdminRequestActivityEndpoints(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)
	store, err := activity.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store.Start("req_fixture", "/v1/responses")
	store.Update("req_fixture", func(record *activity.Record) {
		record.Model = "gpt-6-astra"
		record.AccountID = "acct_fixture"
	})
	if err := store.Finish("req_fixture", "/v1/responses", 200, activity.OutcomeSucceeded, "", ""); err != nil {
		t.Fatal(err)
	}

	app := &App{
		cfg:      config.Config{ProxyAPIKey: "test-key"},
		engine:   gin.New(),
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		activity: store,
	}
	app.routes()

	request := func(path string, want int) map[string]any {
		t.Helper()
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("X-API-Key", "test-key")
		app.Handler().ServeHTTP(recorder, req)
		if recorder.Code != want {
			t.Fatalf("GET %s status = %d, want %d: %s", path, recorder.Code, want, recorder.Body.String())
		}
		var result map[string]any
		if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}

	snapshot := request("/admin/requests/activity", http.StatusOK)
	if len(snapshot["requests"].([]any)) != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	date := time.Now().UTC().Format(time.DateOnly)
	dates := request("/admin/requests/logs/dates", http.StatusOK)
	if len(dates["days"].([]any)) != 1 {
		t.Fatalf("dates = %#v", dates)
	}
	logs := request("/admin/requests/logs?date="+date+"&model=astra", http.StatusOK)
	if len(logs["records"].([]any)) != 1 {
		t.Fatalf("logs = %#v", logs)
	}
	request("/admin/requests/logs?date=bad", http.StatusBadRequest)
	request("/admin/requests/logs?date="+date+"&cursor=bad!", http.StatusBadRequest)
}

func TestAdminRequestActivityStreamStartsWithSnapshot(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)
	store, err := activity.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store.Start("req_live", "/v1/chat/completions")
	app := &App{
		cfg:      config.Config{ProxyAPIKey: "test-key"},
		engine:   gin.New(),
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		activity: store,
	}
	app.routes()
	server := httptest.NewServer(app.Handler())
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/admin/requests/activity/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-API-Key", "test-key")
	res, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK || !strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("status=%d content-type=%q", res.StatusCode, res.Header.Get("Content-Type"))
	}

	reader := bufio.NewReader(res.Body)
	var block strings.Builder
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		block.WriteString(line)
		if line == "\n" {
			break
		}
	}
	if !strings.Contains(block.String(), "event: snapshot") || !strings.Contains(block.String(), `"req_live"`) {
		t.Fatalf("first SSE block = %q", block.String())
	}
}
